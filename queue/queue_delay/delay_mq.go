package queue_delay

import (
	"contrib_cache/cache"
	"github.com/donetkit/contrib-log/glog"
)

type DelayQueue struct {
	client cache.ICache
	logger glog.ILogger
}

func NewDelayQueue(client cache.ICache, logger glog.ILogger) *DelayQueue {
	return &DelayQueue{
		client: client,
		logger: logger,
	}
}

func (r *DelayQueue) GetDelayQueue(topic string) *RedisDelayQueue {
	return New(r.client, topic, r.logger)
}
