package main

import (
	"context"
	"github.com/donetkit/contrib_cache/example/client"
	"log"
	"strconv"
	"time"
)

func main() {
	ctx, c := context.WithTimeout(context.Background(), time.Minute)
	defer c()

	rdb := client.NewClient()

	go func() {
		time.Sleep(time.Second * 5)
		for i := 0; i < 100; i++ {
			time.Sleep(time.Second * 3)
			rdb.WithDB(1).Publish("123", strconv.Itoa(i))
		}
	}()

	PubNub := rdb.WithDB(1).Subscribe("123")

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := PubNub.Receive(ctx)
				if err != nil {
					log.Printf("Receive error: %v", err)
					continue
				}
				log.Println("Subscribe", msg)

			}
		}
	}()
	time.Sleep(time.Hour)

}
