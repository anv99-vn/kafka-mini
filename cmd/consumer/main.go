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
	group := flag.String("group", "test-group", "Consumer group ID")
	topic := flag.String("topic", "test-topic", "Topic to subscribe to")
	seek := flag.Int64("seek", -1, "Seek to offset (default -1)")
	pollTimeout := flag.Int("poll-timeout", 1000, "Poll timeout in ms")
	flag.Parse()

	c, err := client.NewConsumer(*addr, *group)
	if err != nil {
		log.Fatalf("failed to create consumer: %v", err)
	}
	defer c.Close()

	err = c.Subscribe([]string{*topic})
	if err != nil {
		log.Fatalf("failed to subscribe: %v", err)
	}

	if *seek >= 0 {
		err = c.Seek(*topic, 0, *seek)
		if err != nil {
			log.Printf("failed to seek: %v", err)
		} else {
			color.HiMagenta("Seeked topic %s partition 0 to offset %d", *topic, *seek)
		}
	}

	color.HiGreen("Consumer subscribed. Polling for messages...")

	for {
		msgs, err := c.Poll(time.Duration(*pollTimeout) * time.Millisecond)
		if err != nil {
			log.Printf("poll error: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		if len(msgs) > 0 {
			offsets := make(map[string]int64)
			for _, m := range msgs {
				color.HiCyan("Received: %s (topic: %s, partition: %d, offset: %d, key: %s)", 
					string(m.Value), m.Topic, m.Partition, m.Offset, string(m.Key))
				
				tpKey := fmt.Sprintf("%s_%d", m.Topic, m.Partition)
				offsets[tpKey] = m.Offset + 1
			}
			// Automatically commit offsets after successful processing
			err = c.Commit(offsets)
			if err != nil {
				log.Printf("failed to commit: %v", err)
			} else {
				for tp, off := range offsets {
					color.Yellow("Committed offset %d for %s", off, tp)
				}
			}
		} else {
			color.White("No new messages...")
		}
		
		time.Sleep(1 * time.Second)
	}
}
