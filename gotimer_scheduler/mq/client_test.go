package mq

import (
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"testing"
)

func TestGetPulsarClient(t *testing.T) {
	c := GetPulsarClient()
	pro, err := c.Client.CreateProducer(pulsar.ProducerOptions{
		Topic: "topic",
	})
	fmt.Println(pro, err)
}
