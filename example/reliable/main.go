package main

import (
	"contrib_cache/example/client"
	"contrib_cache/queue/queue_reliable"
	"fmt"
	"github.com/donetkit/contrib-log/glog"
	"time"
)

const (
	service     = "redis-mq-test"
	environment = "development" // "production" "development"
	topic       = "Full_Queue_Test_Dev"
)

var logs = glog.New()

func main() {
	//ctx, _ := context.WithCancel(context.Background())

	var redisClient = client.NewClient()

	queueReliable := queue_reliable.NewReliableQueue(redisClient, logs)
	go func() {
		queue1 := queueReliable.GetReliableQueue(topic)
		queue1.DB = 13
		// 清空
		//queue1.ClearAllAck()
		for {
			time.Sleep(time.Millisecond * 100)
			var mqMsg = queue1.TakeOne(10)
			if len(mqMsg) > 0 {
				fmt.Println(mqMsg)
				queue1.Acknowledge(mqMsg)
			}
		}
	}()

	go func() {
		Public(queueReliable, topic)
	}()
	time.Sleep(time.Second * 3600)

}

func Public(queueReliable *queue_reliable.ReliableQueue, topic string) {
	var index = 0
	queue := queueReliable.GetReliableQueue(topic)
	queue.DB = 13
	for {
		queue.Add(fmt.Sprintf("%d", index), 60)
		time.Sleep(time.Millisecond * 50)
		index++
		if index > 20 {
			break
		}
	}
}
