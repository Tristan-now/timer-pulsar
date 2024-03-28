package mq

import (
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"gopkg.in/yaml.v2"
	"log"
	"os"
	"time"
)

type PulsarClient struct {
	Client pulsar.Client
}

type PulsarConfig struct {
	URL string `yaml:"url"`
}

func GetPulsarClient() *PulsarClient {
	configFile, err := os.ReadFile("conf.yml")
	if err != nil {
		log.Fatalf("Unable to read config file: %v", err)
	}

	// 定义配置变量
	var config PulsarConfig

	// 解析YAML配置
	err = yaml.Unmarshal(configFile, &config)
	if err != nil {
		log.Fatalf("Unable to unmarshal config file: %v", err)
	}

	// 使用解析出的URL创建Pulsar客户端
	Client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               config.URL,
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
