package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
)

const (
	zookServer = "127.0.0.1:2181"
	cgroup     = "task"

	// One is zookeeper server, running on default port which is 2181
	// other one is topic, it will passed as flag while running the consumer.go
)

var (
	Topic string
	// variable will be holding kafka topic name, passed as a parameter
)

func main() {

	flag.StringVar(&Topic, "kafka-topic", "task", "Kafka topic")
	flag.Parse()
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	// init consumer
	cg, err := initConsumer()
	if err != nil {
		fmt.Println("Error consumer goup: ", err.Error())
		os.Exit(1)
	}
	defer cg.Close()

	// run consumer
	consume(cg)
}

func initConsumer() (*consumergroup.ConsumerGroup, error) {

	config := consumergroup.NewConfig()
	// config := sarama.NewConfig()
	config.Offsets.Initial = sarama.OffsetOldest
	config.

		// config.ChannelBufferSize = 10000
		config.Offsets.CommitInterval = 5 * time.Second
	// this will commit the message every 5 second

	cg, err := consumergroup.JoinConsumerGroup(cgroup, []string{Topic}, []string{zookServer}, config)
	if err != nil {
		return nil, err
	}

	return cg, err
}

func consume(cg *consumergroup.ConsumerGroup) {
	for {
		select {
		case msg := <-cg.Messages():

			if msg.Topic != Topic {
				continue
			}

			fmt.Println("Topic: ", msg.Topic)
			fmt.Println("Value: ", string(msg.Value))

			err := cg.CommitUpto(msg)
			if err != nil {
				fmt.Println("Error commit zookeeper: ", err.Error())
			}
		}
	}
}
