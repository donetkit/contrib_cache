package client

import (
	"github.com/donetkit/contrib-log/glog"
	"github.com/donetkit/contrib_cache/cache"
	"github.com/donetkit/contrib_cache/redis"
)

func NewClient() cache.ICache {
	log := glog.New()
	return redis.New(redis.WithLogger(log), redis.WithAddr("192.168.5.111"), redis.WithPassword(""))
}
