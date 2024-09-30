package main

import (
	"context"
	"contrib_cache/example/client"
	"contrib_cache/queue/queue_stream"
	"fmt"
	"github.com/donetkit/contrib-log/glog"
	rredis "github.com/redis/go-redis/v9"
	"time"
)

const (
	service     = "redis-mq-test"
	environment = "development" // "production" "development"
	topic       = "Full_Queue_Test_Dev"
)

var logs = glog.New()

func main() {
	ctx, _ := context.WithCancel(context.Background())
	var RedisClient = client.NewClient()
	fullRedis := queue_stream.NewStreamQueue(RedisClient, logs)

	queue1 := fullRedis.GetStreamQueue(topic)
	queue1.BlockTime = 5
	queue1.SetGroup("Group1")
	queue1.ConsumeBlock(ctx, Consumer1)

	queue2 := fullRedis.GetStreamQueue(topic)
	queue2.SetGroup("Group2")
	queue2.ConsumeBlock(ctx, Consumer2)

	go func() {
		Public(fullRedis, topic)
	}()

	//cancel()

	time.Sleep(time.Second * 3600)

}

type MyModel struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

func Public(fullRedis *queue_stream.StreamQueue, topic string) {
	var index = 0
	queue1 := fullRedis.GetStreamQueue(topic)
	queue1.MaxLength = 1000
	for {
		queue1.Add(MyModel{Id: fmt.Sprintf("%d", index), Name: fmt.Sprintf("掌聲%d", index)})
		time.Sleep(time.Millisecond * 10)
		index++
		if index > 20 {
			break
		}
	}
}

func Consumer1(msg []rredis.XMessage) bool {
	logs.Debug("==========Consumer1==========", msg)
	return true
}

func Consumer2(msg []rredis.XMessage) bool {
	logs.Debug("==========Consumer2==========", msg)
	return true
}
