package queue_stream

import (
	"contrib_cache/cache"
	"github.com/donetkit/contrib-log/glog"
)

type StreamQueue struct {
	client cache.ICache
	logger glog.ILogger
}

func NewStreamQueue(client cache.ICache, logger glog.ILogger) *StreamQueue {
	return &StreamQueue{
		client: client,
		logger: logger,
	}
}

func (r *StreamQueue) GetStreamQueue(topic string) *RedisStream {
	return New(r.client, topic, r.logger)
}
