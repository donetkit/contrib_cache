package queue_stream

import (
	"context"
	"fmt"
	"github.com/donetkit/contrib-log/glog"
	"github.com/donetkit/contrib_cache/cache"
	"github.com/redis/go-redis/v9"
	"github.com/shirou/gopsutil/host"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type RedisStream struct {
	ctx                         context.Context   // Context
	DB                          int               // redis DB 默认为 0
	key                         string            // 消息队列key
	Topic                       string            // 消息队列主题
	ThrowOnFailure              bool              // 失败时抛出异常。默认false
	RetryTimesWhenSendFailed    int               // 发送消息失败时的重试次数。默认3次
	RetryIntervalWhenSendFailed int               // 重试间隔。默认1000ms
	count                       int64             // 数量
	RetryInterval               int64             // 重新处理确认队列中死信的间隔。默认60s
	MaxLength                   int64             // 最大队列长度。要保留的消息个数，超过则移除较老消息，非精确，实际上略大于该值，默认100万
	MaxRetry                    int64             // 最大重试次数。超过该次数后，消息将被抛弃，默认10次
	BlockTime                   int64             // 异步消费时的阻塞时间。默认15秒
	StartId                     string            // 开始编号。独立消费时使用，消费组消费时不使用，默认0-0
	Group                       string            // 消费者组。指定消费组后，不再使用独立消费。通过SetGroup可自动创建消费组
	consumer                    string            // 消费者
	client                      cache.ICache      // redis client
	FromLastOffset              bool              // 首次消费时的消费策略  默认值false，表示从头部开始消费，等同于RocketMQ/Java版的CONSUME_FROM_FIRST_OFFSET  一个新的订阅组第一次启动从队列的最前位置开始消费，后续再启动接着上次消费的进度开始消费。
	setGroupId                  int64             // 设置消费组Id
	logger                      glog.ILoggerEntry // logger
}

func New(client cache.ICache, key string, logger glog.ILogger) *RedisStream {
	info, _ := host.Info()
	return &RedisStream{
		RetryTimesWhenSendFailed:    3,
		RetryIntervalWhenSendFailed: 1000,
		logger:                      logger.WithField("MQ-Redis-Stream", "MQ-Redis-Stream"),
		consumer:                    fmt.Sprintf("%s@%d", info.Hostname, os.Getpid()),
		key:                         key,
		Topic:                       key,
		RetryInterval:               60,
		MaxLength:                   1_000_000,
		MaxRetry:                    10,
		BlockTime:                   15,
		StartId:                     "0-0",
		client:                      client,
		ctx:                         context.Background(),
	}
}

// Count 个数
func (r *RedisStream) Count() int64 {
	return r.client.WithDB(r.DB).WithContext(r.ctx).XLen(r.key)
}

// IsEmpty 集合是否为空
func (r *RedisStream) IsEmpty() bool {
	return r.Count() == 0

}

// SetGroup 设置消费组。如果消费组不存在则创建
func (r *RedisStream) SetGroup(group string) bool {
	if len(group) == 0 {
		return false
	}
	r.Group = group

	keyCount := r.client.WithDB(r.DB).WithContext(r.ctx).Exists(r.key)
	// 如果Stream不存在，则直接创建消费组，此时会创建Stream
	if keyCount == 0 {
		return r.GroupCreate(group)
	}

	groups := r.GetGroups()
	if groups == nil {
		return r.GroupCreate(group)
	}
	groupCreate := false
	for _, g := range groups {
		if g.Name == group {
			groupCreate = true
			break
		}
	}
	if !groupCreate {
		return r.GroupCreate(group)
	}

	return false
}

// GetGroups 获取消费分组信息
func (r *RedisStream) GetGroups() []redis.XInfoGroup {
	return r.client.WithDB(r.DB).WithContext(r.ctx).XInfoGroups(r.key)
}

// GroupCreate 创建消费组
// group 消费组名称开始编号。
// startIds 0表示从开头，$表示从末尾，收到下一条生产消息才开始消费 stream不存在，则会报错，所以在后面 加上 MkStream
func (r *RedisStream) GroupCreate(group string, startIds ...string) bool {
	if len(group) == 0 {
		return false
	}
	startId := "0"
	if len(startIds) > 0 {
		startId = startIds[0]
	}

	return r.client.WithDB(r.DB).WithContext(r.ctx).XGroupCreateMkStream(r.key, group, startId) == "OK"
}

// GroupDestroy 销毁消费组
// group 消费组名称
func (r *RedisStream) GroupDestroy(group string) int64 {
	return r.client.WithDB(r.DB).WithContext(r.ctx).XGroupDestroy(r.key, group)
}

// Pending 获取等待列表消息
// group 消费组名称
func (r *RedisStream) Pending(group string, startId string, endId string, count ...int64) []redis.XPendingExt {
	if len(group) == 0 {
		return nil
	}
	if len(startId) == 0 {
		startId = "-"
	}
	if len(endId) == 0 {
		endId = "+"
	}
	var pendingCount int64 = 100
	if len(count) > 0 {
		pendingCount = count[0]
	}
	return r.client.WithDB(r.DB).WithContext(r.ctx).XPendingExt(r.key, group, startId, endId, pendingCount)
}

// GetPending 获取等待列表
// group 消费组名称信息
func (r *RedisStream) GetPending(group string) *redis.XPending {
	if len(group) == 0 {
		return nil
	}
	return r.client.WithDB(r.DB).WithContext(r.ctx).XPending(r.key, group)
}

// GroupDeleteConsumer 销毁消费者
// group 消费组名称
// consumer 消费者
// returns 返回消费者在被删除之前所拥有的待处理消息数量
func (r *RedisStream) GroupDeleteConsumer(group string, consumer string) int64 {
	if len(group) == 0 {
		return 0
	}
	if len(consumer) == 0 {
		return 0
	}
	return r.client.WithDB(r.DB).WithContext(r.ctx).XGroupDelConsumer(r.key, group, consumer)
}

// GroupSetId 设置消费组Id group 消费组名称 startId 开始编号
func (r *RedisStream) GroupSetId(group string, startId string) bool {
	if len(group) == 0 {
		return false
	}
	if len(startId) == 0 {
		startId = "$"
	}
	return r.client.WithDB(r.DB).WithContext(r.ctx).XGroupSetID(r.key, group, startId) == "OK"
}

// ReadGroup 消费组消费  group 消费组名称  consumer 消费组  count 消息个数
func (r *RedisStream) ReadGroup(group string, consumer string, count int64) []redis.XMessage {
	if len(group) == 0 {
		return nil
	}
	if r.FromLastOffset && r.setGroupId == 0 && atomic.CompareAndSwapInt64(&r.setGroupId, 0, 1) {
		r.GroupSetId(r.Group, "$")
		atomic.AddInt64(&r.setGroupId, 1)
	}
	return r.client.WithDB(r.DB).WithContext(r.ctx).XReadGroup(r.key, group, consumer, count, 0)
}

// ReadGroupBlock 消费组消费
// group 消费组
// consumer 消费组
// count 消息个数
// block 阻塞毫秒数，0表示永远
// id 消息id
func (r *RedisStream) ReadGroupBlock(group string, consumer string, count int64, block int64, id ...string) []redis.XMessage {
	if len(group) == 0 {
		return nil
	}

	if r.FromLastOffset && r.setGroupId == 0 && atomic.CompareAndSwapInt64(&r.setGroupId, 0, 1) {
		r.GroupSetId(r.Group, "$")
		atomic.AddInt64(&r.setGroupId, 1)
	}

	arg := &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Count:    count,
		Block:    time.Millisecond * time.Duration(block),
		Streams:  []string{r.key, ">"},
	}

	if len(id) > 0 {
		arg.Streams = []string{r.key, id[0]}
	}

	return r.client.WithDB(r.DB).WithContext(r.ctx).XReadGroup(r.key, group, consumer, count, block, id...)
}

// GetInfo 队列信息
func (r *RedisStream) GetInfo() *redis.XInfoStream {
	return r.client.WithDB(r.DB).WithContext(r.ctx).XInfoStream(r.key)
}

// GetConsumers 获取消费者
// group 消费组
func (r *RedisStream) GetConsumers(group string) []redis.XInfoConsumer {
	if len(group) == 0 {
		return nil
	}
	return r.client.WithDB(r.DB).WithContext(r.ctx).XInfoConsumers(r.key, group)
}

// Acknowledge 消费确认
func (r *RedisStream) Acknowledge(keys ...string) int64 {
	var rs int64
	for _, key := range keys {
		rs += r.Ack(r.Group, key)
	}
	return rs
}

// Add 生产添加
func (r *RedisStream) Add(value interface{}, msgId ...string) string {
	if value == nil {
		return "" //, errors.New("argument null exception error: value is null")
	}

	// 自动修剪超长部分，每1000次生产，修剪一次
	if r.count <= 0 {
		r.count = r.Count()
	}
	atomic.AddInt64(&r.count, 1)

	var trim = false
	if r.MaxLength > 0 {
		if r.MaxLength < 1000 && r.count%r.MaxLength*2 == 0 {
			r.count = r.Count() + 1
			trim = true
		}
		if r.MaxLength > 1000 && r.count%1000 == 0 {
			r.count = r.Count() + 1
			trim = true
		}
	}

	var id = ""
	if len(msgId) > 0 {
		id = msgId[0]
	}
	return r.AddInternal(value, id, trim, true)
}

func (r *RedisStream) AddInternal(value interface{}, msgId string, trim bool, retryOnFailed bool) string {
	for i := 0; i < r.RetryTimesWhenSendFailed; i++ {
		var id = r.client.WithDB(r.DB).WithContext(r.ctx).XAdd(r.key, msgId, trim, r.MaxLength, value)
		if id != "" || !retryOnFailed {
			return id
		}
		if i < r.RetryTimesWhenSendFailed {
			time.Sleep(time.Second * time.Duration(r.RetryIntervalWhenSendFailed))
		}
	}

	return ""
}

// Adds 批量生产添加
func (r *RedisStream) Adds(values []interface{}) int {
	if len(values) == 0 {
		return 0
	}
	// 量少时直接插入，而不用管道
	if len(values) <= 2 {
		for _, value := range values {
			r.Add(value)
		}
		return len(values)
	}

	// 自动修剪超长部分，每1000次生产，修剪一次
	if r.count <= 0 {
		r.count = r.Count()
	}

	var trim = false
	if r.MaxLength > 0 && r.count > 0 && r.count%1000 == 0 {
		r.count = r.Count() + 1
		trim = true
	}

	// 开启管道
	pipe := r.client.WithDB(r.DB).WithContext(r.ctx).Pipeline()
	for _, item := range values {
		atomic.AddInt64(&r.count, 1)
		r.AddInternal(item, "", trim, false)
		trim = false
	}
	cmder, _ := pipe.Exec(r.ctx)

	perm, err := cmder[0].(*redis.StringCmd).Result()
	if err == redis.Nil {

	}
	if r.logger != nil {
		r.logger.Debug("从redis中获取到的值：", perm)
	}

	return len(values)

}

// Take 批量消费获取，前移指针StartId
func (r *RedisStream) Take(count int64) []redis.XMessage {
	var group = r.Group
	var rs []redis.XMessage
	if !(len(group) == 0) {
		r.RetryAck()
		rs = r.ReadGroup(group, r.consumer, count)
	} else {
		rs = r.Read(r.StartId, count)
	}

	if len(group) == 0 && len(rs) > 0 {
		r.SetNextId(rs[len(rs)-1].ID)
	}
	return rs
}

// TakeOne 消费获取一个
func (r *RedisStream) TakeOne() []redis.XMessage {
	return r.Take(1)
}

// TakeOneBlock 异步消费获取一个
func (r *RedisStream) TakeOneBlock(ctx context.Context, timeout int64) []redis.XMessage {
	return r.TakeMessageBlock(1, timeout)
}

func (r *RedisStream) SetNextId(id string) {
	r.StartId = id
}

var _nextRetry time.Time

// RetryAck 处理未确认的死信，重新放入队列
func (r *RedisStream) RetryAck() int {
	var count = 0
	var now = time.Now()
	// 一定间隔处理当前key死信
	if _nextRetry.UnixMilli() < now.UnixMilli() {
		_nextRetry = now.Add(time.Duration(r.RetryInterval) * time.Second)
		var retry = time.Duration(r.RetryInterval*1000) * time.Millisecond
		// 拿到死信，重新放入队列
		id := ""
		for {
			var listXPendingExt = r.Pending(r.Group, id, "", 100)
			if len(listXPendingExt) == 0 {
				break
			}
			for _, xPendingExt := range listXPendingExt {
				if xPendingExt.Idle >= retry {
					if xPendingExt.RetryCount > r.MaxRetry {
						if r.logger != nil {
							r.logger.Debug(fmt.Sprintf("%s 删除多次失败死信：%v", r.Group, xPendingExt))
						}
						//Delete(item.Id);
						r.Claim(r.Group, r.consumer, xPendingExt.ID, r.RetryInterval*1000)
						r.Ack(r.Group, xPendingExt.ID)

					} else {
						if r.logger != nil {
							r.logger.Debug(fmt.Sprintf("%s 定时回滚：%v", r.Group, xPendingExt))
						}
						r.Claim(r.Group, r.consumer, xPendingExt.ID, r.RetryInterval*1000)
						count++
					}
				}

			}

			// 下一个开始id
			id = listXPendingExt[len(listXPendingExt)-1].ID
			var p = strings.Index(id, "-")
			if p > 0 {
				nid, err := strconv.ParseInt(id[:p], 10, 64)
				if err == nil {
					id = fmt.Sprintf("%d-0", nid+1)
				}
			}

		}
		// 清理历史消费者
		consumers := r.GetConsumers(r.Group)
		for _, item := range consumers {
			if item.Pending == 0 && item.Idle > 3600_000 {
				if r.logger != nil {
					r.logger.Debug(fmt.Sprintf("%s 删除空闲消费者：%v", r.Group, item))
				}
				r.GroupDeleteConsumer(r.Group, item.Name)
			}
		}
	}
	return count
}

// Read 原始独立消费
// startId 开始编号 特殊的$，表示接收从阻塞那一刻开始添加到流的消息
// count 消息个数
func (r *RedisStream) Read(startId string, count int64) []redis.XMessage {
	return r.client.WithDB(r.DB).WithContext(r.ctx).XRead(r.key, startId, count, 0)
}

// ReadBlock 原始独立消费
// startId 开始编号 特殊的$，表示接收从阻塞那一刻开始添加到流的消息
// count 消息个数
// block 阻塞毫秒数，0表示永远
func (r *RedisStream) ReadBlock(startId string, count int64, block int64) []redis.XMessage {
	return r.client.WithDB(r.DB).WithContext(r.ctx).XRead(r.key, startId, count, block)
}

// Claim 改变待处理消息的所有权，抢夺他人未确认消息
// group 消费组名称
// consumer 目标消费者
// id 消息Id
// msIdle 空闲时间。默认3600_000
func (r *RedisStream) Claim(group string, consumer string, id string, msIdle int64) []redis.XMessage {
	if len(group) == 0 || len(consumer) == 0 {
		return nil
	}
	return r.client.WithDB(r.DB).WithContext(r.ctx).XClaim(r.key, group, consumer, id, msIdle)

}

// Ack 确认消息
// group 消费组名称
// id 消息Id
func (r *RedisStream) Ack(group string, id string) int64 {
	if len(group) == 0 {
		return 0
	}
	if len(id) == 0 {
		return 0
	}
	return r.client.WithDB(r.DB).WithContext(r.ctx).XAck(r.key, group, id)
}

// TakeMessageBlock 异步消费获取一个 timeout 超时时间，默认0秒永远阻塞
func (r *RedisStream) TakeMessageBlock(count int64, timeout int64) []redis.XMessage {
	var group = r.Group
	if r.FromLastOffset && r.setGroupId == 0 && atomic.CompareAndSwapInt64(&r.setGroupId, 0, 1) {
		r.GroupSetId(r.Group, "$")
		atomic.AddInt64(&r.setGroupId, 1)
	}

	if !(len(group) == 0) {
		r.RetryAck()
	}

	var rs []redis.XMessage
	var block = timeout * 1000
	if !(len(group) == 0) {
		rs = r.ReadGroupBlock(group, r.consumer, count, block, ">")
	} else {
		rs = r.ReadBlock(r.StartId, count, block)
	}

	if len(rs) == 0 {
		// id为>时，消费从未传递给消费者的消息
		// id为$时，消费从从阻塞开始新收到的消息
		// id为0时，消费当前消费者的历史待处理消息，包括自己未ACK和从其它消费者抢来的消息
		// 使用消费组时，如果拿不到消息，则尝试消费抢过来的历史消息
		if !(len(group) == 0) {
			rs = r.ReadGroupBlock(group, r.consumer, count, 3_000, "0")
			if len(rs) == 0 {
				return nil
			}
			for _, val := range rs {
				if r.logger != nil {
					r.logger.Debug(fmt.Sprintf("%s 处理历史：%s", r.Group, val.ID))
				}
			}
			return rs
		}
	}
	// 全局消费（非消费组）时，更新编号
	if len(group) == 0 && len(rs) > 0 {
		r.SetNextId(rs[len(rs)-1].ID)
	}
	return rs
}

// ConsumeBlock 队列消费大循环，处理消息后自动确认
// ctx context.Context
// OnMessage msg 消费消息信息 returns 返回True确认消费Acknowledge 返回False不确认消费Acknowledge 等待二次消费
func (r *RedisStream) ConsumeBlock(ctx context.Context, OnMessage func(msg []redis.XMessage) bool) {
	go func() {
		// 自动创建消费组
		r.SetGroup(r.Group)
		// 主题
		//var topic = r.key
		// 超时时间，用于阻塞等待
		var timeout = r.BlockTime

		for {
			select {
			case <-ctx.Done():
				return // 退出了...
			default:
				// 异步阻塞消费
				mqMsg := r.TakeMessageBlock(1, timeout)
				if len(mqMsg) == 0 { // 没有消息，歇一会
					time.Sleep(time.Millisecond * 1000)
					continue
				}

				// 处理消息
				result := OnMessage(mqMsg)
				if result {
					// 确认消息
					for _, msg := range mqMsg {
						r.Acknowledge(msg.ID)
					}
				}
			}

		}
	}()

}

// Delete 删除指定消息
// id 消息Id
func (r *RedisStream) Delete(id ...string) int64 {
	if len(id) == 0 {
		return 0
	}
	return r.client.WithDB(r.DB).WithContext(r.ctx).XDel(r.key, id...)
}

// Trim 裁剪队列到指定大小
// maxLen 最大长度。为了提高效率，最大长度并没有那么精准
func (r *RedisStream) Trim(maxLen int64) int64 {
	return r.client.WithDB(r.DB).WithContext(r.ctx).XTrimMaxLen(r.key, maxLen)
}

// Range  获取区间消息
func (r *RedisStream) Range(startId string, endId string, count ...int64) []redis.XMessage {
	if len(startId) == 0 {
		startId = "-"
	}
	if len(endId) == 0 {
		endId = "+"
	}

	if len(count) > 0 {
		return r.client.WithDB(r.DB).WithContext(r.ctx).XRangeN(r.key, startId, endId, count[0])
	}

	return r.client.WithDB(r.DB).WithContext(r.ctx).XRange(r.key, startId, endId)
}

// RangeTimeSpan 获取区间消息
func (r *RedisStream) RangeTimeSpan(start int64, end int64, count ...int64) []redis.XMessage {
	return r.Range(fmt.Sprintf("%d-0", start), fmt.Sprintf("%d-0", end), count...)
}
