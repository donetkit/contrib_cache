package cache

import (
	"context"
	"github.com/redis/go-redis/v9"
	"time"
)

type IMemoryCache interface {
	WithDB(db int) ICache
	WithContext(ctx context.Context) ICache
	Get(key string) interface{}
	GetString(key string) (string, error)
	Set(key string, value interface{}, timeOut time.Duration) error
	IsExist(key string) bool
	Delete(key string) (int64, error)
	Increment(key string, value int64) (int64, error)
	IncrementFloat(key string, value float64) (float64, error)
	Decrement(key string, value int64) (int64, error)
	Flush()
}

type ICache interface {
	WithDB(db int) ICache
	WithContext(ctx context.Context) ICache
	Get(key string, val ...any) interface{}
	GetString(key string) (string, error)

	Set(key string, value interface{}, timeOut time.Duration) error
	SetString(key string, value string, timeOut time.Duration) error
	SetEX(key string, val interface{}, timeOut time.Duration) error

	IsExist(key string) bool
	Delete(key ...string) int64

	LPush(key string, value ...interface{}) int64
	RPop(key string) string

	BRPopLPush(source string, destination string, timeOut time.Duration) string
	RPopLPush(source string, destination string) string
	LRem(key string, count int64, value interface{}) int64
	Scan(cursor uint64, match string, count int64) ([]string, uint64)
	SetNX(key string, value interface{}, expiration time.Duration) bool
	LRange(key string, start int64, stop int64) []string

	XRead(key string, startId string, count int64, block int64) []redis.XMessage
	XAdd(key, msgId string, trim bool, maxLength int64, value interface{}) string
	XAddKey(key, msgId string, trim bool, maxLength int64, vKey string, value interface{}) string
	XDel(key string, id ...string) int64
	GetLock(lockName string, acquireTimeout, lockTimeOut time.Duration) (string, error)
	ReleaseLock(lockName, code string) bool

	Increment(key string, value int64) (int64, error)
	IncrementFloat(key string, value float64) (float64, error)
	Decrement(key string, value int64) (int64, error)

	Flush()

	ZAdd(key string, score float64, value ...interface{}) int64
	ZRangeByScore(key string, min int64, max int64, offset int64, count int64) []string
	ZRem(key string, value ...interface{}) int64

	XLen(key string) int64
	Exists(keys ...string) int64
	XInfoGroups(key string) []redis.XInfoGroup
	XGroupCreateMkStream(key string, group string, start string) string
	XGroupDestroy(key string, group string) int64
	XPendingExt(key string, group string, startId string, endId string, count int64, consumer ...string) []redis.XPendingExt
	XPending(key string, group string) *redis.XPending
	XGroupDelConsumer(key string, group string, consumer string) int64
	XGroupSetID(key string, group string, start string) string
	XReadGroup(key string, group string, consumer string, count int64, block int64, id ...string) []redis.XMessage
	XInfoStream(key string) *redis.XInfoStream
	XInfoConsumers(key string, group string) []redis.XInfoConsumer
	Pipeline() redis.Pipeliner

	XClaim(key string, group string, consumer string, id string, msIdle int64) []redis.XMessage
	XAck(key string, group string, ids ...string) int64
	XTrimMaxLen(key string, maxLen int64) int64
	XRangeN(key string, start string, stop string, count int64) []redis.XMessage
	XRange(key string, start string, stop string) []redis.XMessage

	HashGet(key, value string) string
	HashGets(key string, value ...string) []interface{}
	HashAll(key string) map[string]string
	HashSet(key string, values ...interface{}) int64
	HashExist(key, values string) bool
	HashDel(key string, values ...string) int64
	HashKeys(key string) []string
	HashLen(key string) int64
}
