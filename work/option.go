package work

import (
	"context"
	"github.com/donetkit/contrib-log/glog"
)

type config struct {
	ctx      context.Context
	logger   glog.ILoggerEntry
	addr     string
	port     int
	password string
	db       int
}

// Option specifies instrumentation configuration options.
//type Option interface {
//	apply(*config)
//}

type Option func(p *config)

// WithLogger prevents logger.
func WithLogger(logger glog.ILogger) Option {
	return func(cfg *config) {
		cfg.logger = logger.WithField("Cache-Redis", "Cache-Redis")
	}
}

// WithAddr prevents addr.
func WithAddr(addr string) Option {
	return func(cfg *config) {
		cfg.addr = addr
	}
}

// WithPort prevents port.
func WithPort(port int) Option {
	return func(cfg *config) {
		cfg.port = port
	}
}

// WithPassword prevents password.
func WithPassword(password string) Option {
	return func(cfg *config) {
		cfg.password = password
	}
}

// WithDB prevents db.
func WithDB(db int) Option {
	return func(cfg *config) {
		cfg.db = db
	}
}
