package cache

import (
	"context"
	"time"
)

// WorkCache interface
type WorkCache interface {
	WithDB(db int) WorkCache
	WithContext(ctx context.Context) WorkCache
	Get(key string) interface{}
	Set(key string, val interface{}, timeout time.Duration) error
	IsExist(key string) bool
	Delete(key string) error
}

// ContextCache interface
type ContextCache interface {
	WorkCache
	GetContext(ctx context.Context, key string) interface{}
	SetContext(ctx context.Context, key string, val interface{}, timeout time.Duration) error
	IsExistContext(ctx context.Context, key string) bool
	DeleteContext(ctx context.Context, key string) error
}

// GetContext get value from cache
func GetContext(ctx context.Context, cache WorkCache, key string) interface{} {
	if c, ok := cache.(ContextCache); ok {
		return c.GetContext(ctx, key)
	}
	return cache.Get(key)
}

// SetContext set value to cache
func SetContext(ctx context.Context, cache WorkCache, key string, val interface{}, timeout time.Duration) error {
	if c, ok := cache.(ContextCache); ok {
		return c.SetContext(ctx, key, val, timeout)
	}
	return cache.Set(key, val, timeout)
}

// IsExistContext check value exists in cache.
func IsExistContext(ctx context.Context, cache WorkCache, key string) bool {
	if c, ok := cache.(ContextCache); ok {
		return c.IsExistContext(ctx, key)
	}
	return cache.IsExist(key)
}

// DeleteContext delete value in cache.
func DeleteContext(ctx context.Context, cache WorkCache, key string) error {
	if c, ok := cache.(ContextCache); ok {
		return c.DeleteContext(ctx, key)
	}
	return cache.Delete(key)
}
