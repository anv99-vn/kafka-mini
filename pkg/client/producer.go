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
	for i := 0; i < len(p.addrs); i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		res, err := p.client.Send(ctx, req)
		cancel()

		if err == nil {
			return res, nil
		}

		lastErr = err
		// Rotate to next broker
		p.currIdx = (p.currIdx + 1) % len(p.addrs)
		_ = p.connect()
	}

	return nil, fmt.Errorf("all brokers failed, last error: %v", lastErr)
}

func (p *Producer) Close() error {
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}
