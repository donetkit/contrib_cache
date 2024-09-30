package redis

import (
	"context"
	"errors"
	"fmt"
	"github.com/donetkit/contrib-log/glog"
	"github.com/redis/go-redis/v9"
	"net"
	"strings"
)

type TracingHook struct {
	logger glog.ILoggerEntry
}

func (h *TracingHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := next(ctx, network, addr)
		return conn, err
	}
}

func (h *TracingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		dbValue := ""
		db, ok := ctx.Value(redisClientDBKey).(int)
		if ok {
			dbValue = fmt.Sprintf("[%d]", db)
		}

		err := next(ctx, cmd)

		if h.logger != nil {
			h.logger.Debugf("db%s:%s ", dbValue, cmd.String())
		}

		if cmdErr := cmd.Err(); cmdErr != nil {
			h.recordError(cmdErr)
		}

		return err
	}

}

func (h *TracingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		dbValue := ""
		db, ok := ctx.Value(redisClientDBKey).(int)
		if ok {
			dbValue = fmt.Sprintf("[%d]", db)
		}

		err := next(ctx, cmds)

		if h.logger != nil {
			for _, c := range cmds {
				h.logger.Debugf("db%s:%s ", dbValue, c.String())
			}
		}

		if cmdErr := cmds[0].Err(); cmdErr != nil {
			h.recordError(cmdErr)
		}

		return err
	}
}

func newLogHook(logger glog.ILoggerEntry) *TracingHook {
	hook := &TracingHook{
		logger: logger,
	}
	return hook
}

func (h *TracingHook) recordError(err error) {
	if !errors.Is(err, redis.Nil) {
		if h.logger != nil {
			h.logger.Error(err.Error())
		}
	}
}

func getTraceFullName(cmd redis.Cmder, dbValue string) string {
	var args = cmd.Args()
	switch name := cmd.Name(); name {
	case "cluster", "command":
		if len(args) == 1 {
			return fmt.Sprintf("db%s:redis:%s", dbValue, name)
		}
		if s2, ok := args[1].(string); ok {
			return fmt.Sprintf("db%s:redis:%s => %s", dbValue, name, s2)
		}
		return fmt.Sprintf("db%s:redis:%s", dbValue, name)
	default:
		if len(args) == 1 {
			return fmt.Sprintf("db%s:redis:%s", dbValue, name)
		}
		if s2, ok := args[1].(string); ok {
			return fmt.Sprintf("db%s:redis:%s => %s", dbValue, name, s2)
		}
		return fmt.Sprintf("db%s:redis:%s", dbValue, name)
	}
}

func getTraceFullNames(cmd []redis.Cmder, dbValue string) string {
	var cmdStr []string
	for _, c := range cmd {
		cmdStr = append(cmdStr, getTraceFullName(c, dbValue))
	}
	return strings.Join(cmdStr, ", ")
}
