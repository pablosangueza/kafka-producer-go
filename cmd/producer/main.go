package main

import (
	"bufio"
	"fmt"
	"log"
	"os"

	"github.com/Shopify/sarama"
)

func main() {
	// create a Kafka producer configuration
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	// create a Kafka producer
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Error creating Kafka producer: %s", err.Error())
	}
	defer producer.Close()

	reader := bufio.NewReader(os.Stdin)

	for {
		var input string
		fmt.Print("Enter a message: ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading string:", err)
			return
		}
		// send a message to a Kafka topic
		message := &sarama.ProducerMessage{
			Topic: "test",
			Value: sarama.StringEncoder(input),
		}
		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			log.Fatalf("Error sending message to Kafka: %s", err.Error())
		}
		log.Printf("Message sent to partition %d at offset %d", partition, offset)
	}

}
