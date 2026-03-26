package client

import (
	"context"
	"fmt"
	"strings"
	"time"

	pb "github.com/vna/kafka-mini/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Producer struct {
	addrs      []string
	currIdx    int
	client     pb.ProducerServiceClient
	conn       *grpc.ClientConn
	nextOffset int64 // Added for local offset tracking
}

func NewProducer(addrs string) (*Producer, error) {
	addrList := strings.Split(addrs, ",")
	p := &Producer{
		addrs:   addrList,
		currIdx: 0,
	}
	
	// Fallback connection attempt
	var lastErr error
	for i := 0; i < len(p.addrs); i++ {
		if err := p.connect(); err == nil {
			return p, nil
		} else {
			lastErr = err
			p.currIdx = (p.currIdx + 1) % len(p.addrs)
		}
	}
	return nil, fmt.Errorf("failed to connect to any broker: %v", lastErr)
}

func (p *Producer) connect() error {
	if p.conn != nil {
		p.conn.Close()
	}
	addr := p.addrs[p.currIdx]
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	p.conn = conn
	p.client = pb.NewProducerServiceClient(conn)
	return nil
}

func (p *Producer) Send(topic string, key []byte, values ...[]byte) (*pb.SendResponse, error) {
	var messages []*pb.Message
	now := time.Now().UnixMilli()
	for _, v := range values {
		messages = append(messages, &pb.Message{
			Topic:     topic,
			Key:       key,
			Value:     v,
			Offset:    p.nextOffset,
			Timestamp: now,
		})
		p.nextOffset++
	}

	req := &pb.SendRequest{
		Topic:    topic,
		Key:      key,
		Messages: messages,
	}

	var lastErr error
	maxRetries := 10 // Overall limit, allowing time for new leader to be elected
	
	for r := 0; r < maxRetries; r++ {
		for i := 0; i < len(p.addrs); i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			res, err := p.client.Send(ctx, req)
			cancel()

			if err == nil {
				return res, nil
			}

			lastErr = err
			
			// Detect if there's a leader hint
			if strings.Contains(err.Error(), "hint:") {
				parts := strings.Split(err.Error(), "hint: ")
				if len(parts) > 1 {
					hintAddr := strings.TrimSpace(parts[1])
					// Find if hintAddr is in our list
					for idx, a := range p.addrs {
						if a == hintAddr || ":" + a == hintAddr || a == ":" + hintAddr {
							fmt.Printf("Received hint: leader is at %s. Jumping to it...\n", hintAddr)
							p.currIdx = idx
							_ = p.connect()
							break
						}
					}
					// If we found a hint, don't just increment currIdx, retry on the hinted node immediately
					continue 
				}
			}

			// We encountered an error (e.g., connection lost).
			// Rotate to next broker
			p.currIdx = (p.currIdx + 1) % len(p.addrs)
			_ = p.connect()
		}
		// Wait before trying the cluster again, giving Raft time to elect a new leader
		time.Sleep(300 * time.Millisecond)
	}

	return nil, fmt.Errorf("failed after retries, last error: %v", lastErr)
}

func (p *Producer) Close() error {
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}
