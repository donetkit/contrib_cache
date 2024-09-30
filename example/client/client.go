package client

import (
	"contrib_cache/cache"
	"contrib_cache/redis"
	"github.com/donetkit/contrib-log/glog"
)

func NewClient() cache.ICache {
	log := glog.New()
	return redis.New(redis.WithLogger(log))
}
