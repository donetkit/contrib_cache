package main

import (
	"fmt"
	"github.com/donetkit/contrib-log/glog"
	"github.com/donetkit/contrib_cache/example/client"
	"github.com/donetkit/contrib_cache/queue/queue_delay"
	"time"
)

const (
	service     = "redis-mq-test"
	environment = "development" // "production" "development"
	topic       = "Full_Queue_Test_Dev"
)

var logs = glog.New()

func main() {

	var RedisClient = client.NewClient()

	delayQueue := queue_delay.NewDelayQueue(RedisClient, logs)
	go func() {
		queue1 := delayQueue.GetDelayQueue(topic)
		for {
			var msg = queue1.TakeOne(10)
			if len(msg) > 0 {
				fmt.Println("TakeOne= ", msg)
			}
		}
	}()

	go func() {
		Public(delayQueue, topic)
	}()

	time.Sleep(time.Second * 3600)

}

func Public(delayQueue *queue_delay.DelayQueue, topic string) {
	var index = 0
	queue := delayQueue.GetDelayQueue(topic)
	for {
		v := fmt.Sprintf("%d", index)
		fmt.Println("Add= ", v)
		queue.Add(v, 60)
		time.Sleep(time.Millisecond * 1000)
		index++
		if index > 20 {
			break
		}
	}
}
