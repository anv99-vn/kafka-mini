package main

import (
	"flag"
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
			var lastOffset int64
			for _, m := range msgs {
				color.HiCyan("Received: %s (key: %s, offset: %d)", 
					string(m.Value), string(m.Key), m.Offset)
				lastOffset = m.Offset
			}
			// Automatically commit offsets after successful processing
			err = c.Commit(map[string]int64{
				*topic + "_0": lastOffset + 1,
			})
			if err != nil {
				log.Printf("failed to commit: %v", err)
			} else {
				color.Yellow("Committed offset %d", lastOffset+1)
			}
		} else {
			color.White("No new messages...")
		}
		
		time.Sleep(1 * time.Second)
	}
}
