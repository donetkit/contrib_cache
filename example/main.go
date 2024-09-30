package main

import (
	"context"
	"contrib_cache/cache"
	"contrib_cache/example/client"
	"fmt"
	"github.com/donetkit/contrib-log/glog"
	"time"
)

const (
	service     = "redis-test"
	environment = "development" // "production" "development"
)

func main() {
	ctx := context.Background()
	log := glog.New()
	rdb := client.NewClient()
	if err := redisCommands(ctx, rdb); err != nil {
		log.Error(err.Error())
		return
	}

	for {
		time.Sleep(time.Second * 5)
		if err := redisCommands(ctx, rdb); err != nil {
			log.Error(err.Error())
			return
		}
	}

	time.Sleep(time.Hour)

}
func redisCommands(ctx context.Context, rdb cache.ICache) error {
	if err := rdb.WithDB(0).WithContext(ctx).Set("foo", "bar", 0); err != nil {
		return err
	}
	rdb.WithDB(0).WithContext(ctx).Get("foo")

	rdb.WithDB(0).WithContext(ctx).HashSet("myhash", map[string]interface{}{"key1": "1", "key2": "2"})
	rdb.WithDB(0).WithContext(ctx).HashSet("myhash", "key3", "3", "key4", "4")
	rdb.WithDB(0).WithContext(ctx).HashSet("myhash", []string{"key5", "5", "key6", "6"})

	fmt.Println(rdb.WithDB(0).WithContext(ctx).HashLen("myhash"))

	fmt.Println(rdb.WithDB(0).WithContext(ctx).HashAll("myhash"))

	fmt.Println(rdb.WithDB(0).WithContext(ctx).HashExist("myhash", "key33"))

	fmt.Println(rdb.WithDB(0).WithContext(ctx).HashGet("myhash", "key1"))

	fmt.Println(rdb.WithDB(0).WithContext(ctx).HashGets("myhash", "key1", "key6"))

	fmt.Println(rdb.WithDB(0).WithContext(ctx).HashKeys("myhash"))

	fmt.Println(rdb.WithDB(0).WithContext(ctx).HashAll("myhash"))

	rdb.WithDB(0).WithContext(ctx).HashDel("myhash", "key33")

	return nil
}
