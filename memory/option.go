package memory

import (
	"context"
	"github.com/donetkit/contrib-log/glog"
	"time"
)

type config struct {
	ctx               context.Context
	logger            glog.ILoggerEntry
	defaultExpiration time.Duration
	cleanupInterval   time.Duration
}

type Option func(p *config)

func WithDefaultExpiration(defaultExpiration time.Duration) Option {
	return func(cfg *config) {
		cfg.defaultExpiration = defaultExpiration
	}
}

func WithCleanupInterval(cleanupInterval time.Duration) Option {
	return func(cfg *config) {
		cfg.cleanupInterval = cleanupInterval
	}
}

// WithLogger prevents logger.
func WithLogger(logger glog.ILogger) Option {
	return func(cfg *config) {
		cfg.logger = logger.WithField("Cache-Memory", "Cache-Memory")
	}
}
