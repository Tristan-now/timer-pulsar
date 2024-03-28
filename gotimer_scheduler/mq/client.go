package mq

import (
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"log"
	"time"
)

type PulsarClient struct {
	Client pulsar.Client
}

func GetPulsarClient() *PulsarClient {
	Client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://10.9.130.50:6650",
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})

	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}
	fmt.Println("get pc")
	return &PulsarClient{
		Client: Client,
	}

}
