package queue_reliable

import (
	"contrib_cache/cache"
	"github.com/donetkit/contrib-log/glog"
)

type ReliableQueue struct {
	client cache.ICache
	logger glog.ILogger
}

func NewReliableQueue(client cache.ICache, logger glog.ILogger) *ReliableQueue {
	return &ReliableQueue{
		client: client,
		logger: logger,
	}
}

func (r *ReliableQueue) GetReliableQueue(topic string) *RedisReliableQueue {
	return New(r.client, topic, r.logger)
}
