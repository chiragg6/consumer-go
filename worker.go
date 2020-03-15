package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"

	"github.com/Shopify/sarama"
)

const (
	kafkaConnection = "127.0.0.1:9092"
)

// type Task struct {
// 	topic string
// }

var Topic string

func main() {

	// configDir := flag.String("configdir", "/go/src/github.com/kafka-client", "Directory where the topic configuration file is stored")
	// flag.Parse()

	// viper.SetConfigName("topic")
	// // viper.SetConfigType("yml")
	// viper.AddConfigPath(*configDir)
	// err := viper.ReadInConfig()
	// if err != nil {
	// 	panic(fmt.Errorf("Unable to read topic config file: %s\n", err))
	// }
	// Code to read topic config file
	flag.StringVar(&Topic, "kafka-topic", "task", "Kafka topic")
	flag.Parse()

	// reading the topic as a parameter while running main.go

	kafkaProducer, err := Producer()
	if err != nil {
		fmt.Println("Error producer: ", err.Error())
		os.Exit(1)
	}
	// task := &Task{}
	// err = viper.Unmarshal(task)
	// if err != nil {
	// 	panic(fmt.Errorf("Unable to unmarshal topic config into struct: %s\n", err))
	// }

	read := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("message: ")
		msg, _ := read.ReadString('\n')

		send(msg, kafkaProducer)

	}
}

func Producer() (sarama.SyncProducer, error) {

	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{kafkaConnection}, config)

	return producer, err
}

func send(message string, producer sarama.SyncProducer) {

	msg := &sarama.ProducerMessage{
		Topic: Topic,
		Value: sarama.StringEncoder(message),
	}
	_, _, err := producer.SendMessage(msg)
	if err != nil {
		fmt.Println("Error in sending: ", err.Error())
	}

}
