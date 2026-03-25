package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/fatih/color"
	"github.com/vna/kafka-mini/pkg/client"
)

func main() {
	addr := flag.String("addr", "localhost:9092", "Broker address")
	topic := flag.String("topic", "test-topic", "Topic name")
	partition := flag.Int("partition", 0, "Partition number")
	key := flag.String("key", "user-batch-1", "Message key")
	message := flag.String("message", "hello kafka-mini", "Message content")
	flag.Parse()

	p, err := client.NewProducer(*addr)
	if err != nil {
		log.Fatalf("failed to create producer: %v", err)
	}
	defer p.Close()

	// Use the provided message, adding a timestamp for testing uniqueness
	msgValue := []byte(fmt.Sprintf("%s at %s", *message, time.Now().Format(time.RFC3339)))
	messages := [][]byte{msgValue}

	resp, err := p.Send(*topic, []byte(*key), messages...)
	if err != nil {
		log.Fatalf("failed to send messages: %v", err)
	}

	color.Cyan("Message sent successfully! Topic: %s, Partition: %d (requested %d), Offsets: %v", 
		*topic, resp.Partition, *partition, resp.Offsets)
}
