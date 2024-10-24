package work

import (
	"context"
	"github.com/donetkit/contrib_cache/cache"
	"reflect"
	"time"
)

func (c *Cache) WithDB(db int) cache.WorkCache {
	if db < 0 || db > 15 {
		db = 0
	}
	cache := &Cache{
		db:     db,
		ctx:    c.ctx,
		client: allClient[db],
		config: c.config,
	}
	cache.ctx = context.WithValue(cache.ctx, redisClientDBKey, db)
	return cache
}

func (c *Cache) WithContext(ctx context.Context) cache.WorkCache {
	if ctx != nil {
		c.ctx = ctx
	} else {
		c.ctx = c.config.ctx
	}
	c.ctx = context.WithValue(c.ctx, redisClientDBKey, c.db)
	return c
}

func (c *Cache) Get(key string) interface{} {
	data, err := c.client.Get(c.ctx, key).Bytes()
	if err != nil {
		return nil
	}
	var reply interface{}
	if err = Unmarshal(data, &reply); err != nil {
		return string(data)
	}
	return reply
}

func (c *Cache) Set(key string, value interface{}, timeOut time.Duration) error {
	val := interfaceToStr(value)
	return c.client.Set(c.ctx, key, val, timeOut).Err()
}

// IsExist 判断key是否存在
func (c *Cache) IsExist(key string) bool {

	i := c.client.Exists(c.ctx, key).Val()
	return i > 0
}

// Delete 删除
func (c *Cache) Delete(key string) error {

	cmd := c.client.Del(c.ctx, key)
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return nil
}

// interfaceToStr
func interfaceToStr(obj interface{}) string {
	if str, ok := obj.(string); ok {
		return str
	}
	v := reflect.ValueOf(obj)
	switch v.Kind() {
	case reflect.String:
		return obj.(string)
	default:

	}
	str, _ := Marshal(obj)
	return string(str)
}
