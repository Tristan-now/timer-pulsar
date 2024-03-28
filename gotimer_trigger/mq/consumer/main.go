package main

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"log"
	"sync"
	"time"
)

//consumer监听mq，模拟触发器抢令牌

var wg sync.WaitGroup

func main() {
	wg.Add(1)
	client, _ := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://localhost:6650",
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})

	consumer := make([]pulsar.Consumer, 110)

	for i := 0; i < 10; i++ {
		consumer[i], _ = client.Subscribe(pulsar.ConsumerOptions{
			Topic:            "scheduler-topic",
			SubscriptionName: "my-sub",
			Type:             pulsar.Shared,
		})

	}

	for i := 0; i < 10; i++ {
		go func() {
			for {
				// may block here
				msg, err := consumer[i].Receive(context.Background())
				if err != nil {
					log.Fatal(err)
				}

				fmt.Printf("Consumer%d Received  '%s'\n", i,
					string(msg.Payload()))

				consumer[i].Ack(msg)
			}
		}()
	}
	for i := 0; i < 10; i++ {

		defer consumer[i].Close()
	}
	wg.Wait()
}
