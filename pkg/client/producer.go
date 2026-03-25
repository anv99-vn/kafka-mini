package client

import (
	"context"
	"time"

	pb "github.com/vna/kafka-mini/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Producer struct {
	client pb.ProducerServiceClient
	conn   *grpc.ClientConn
	nextOffset int64 // Added for local offset tracking
}

func NewProducer(addr string) (*Producer, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	client := pb.NewProducerServiceClient(conn)
	return &Producer{
		client: client,
		conn:   conn,
	}, nil
}

func (p *Producer) Send(topic string, key []byte, values ...[]byte) (*pb.SendResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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

	return p.client.Send(ctx, &pb.SendRequest{
		Topic:    topic,
		Key:      key,
		Messages: messages,
	})
}

func (p *Producer) Close() error {
	return p.conn.Close()
}
