package memory

import (
	"fmt"
	"time"
)

var (
	ErrKeyExists = fmt.Errorf("item already exists")
	ErrCacheMiss = fmt.Errorf("item not found")
)

type item struct {
	Object     interface{}
	Expiration *time.Time
}

// IsExist true if the item has expired or is not exist.
func (i *item) IsExist() bool {
	if i.Expiration == nil {
		return false
	}
	return i.Expiration.Before(time.Now())
}

type janitor struct {
	Interval time.Duration
	stop     chan bool
}
