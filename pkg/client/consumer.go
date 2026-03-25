package client

import (
	"context"
	"time"

	pb "github.com/vna/kafka-mini/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Consumer struct {
	client  pb.ConsumerServiceClient
	conn    *grpc.ClientConn
	groupId string
}

func NewConsumer(addr, groupId string) (*Consumer, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	client := pb.NewConsumerServiceClient(conn)
	return &Consumer{
		client:  client,
		conn:    conn,
		groupId: groupId,
	}, nil
}

func (c *Consumer) Subscribe(topics []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.client.Subscribe(ctx, &pb.SubscribeRequest{
		GroupId: c.groupId,
		Topics:  topics,
	})
	return err
}

func (c *Consumer) Poll(timeout time.Duration) ([]*pb.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout+time.Second)
	defer cancel()

	resp, err := c.client.Poll(ctx, &pb.PollRequest{
		GroupId:   c.groupId,
		TimeoutMs: int64(timeout / time.Millisecond),
	})
	if err != nil {
		return nil, err
	}
	return resp.Messages, nil
}

func (c *Consumer) Commit(offsets map[string]int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.client.Commit(ctx, &pb.CommitRequest{
		GroupId: c.groupId,
		Offsets: offsets,
	})
	return err
}

func (c *Consumer) Seek(topic string, partition int32, offset int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.client.Seek(ctx, &pb.SeekRequest{
		GroupId: c.groupId,
		Partition: &pb.TopicPartition{
			Topic:     topic,
			Partition: partition,
		},
		Offset: offset,
	})
	return err
}

func (c *Consumer) Close() error {
	return c.conn.Close()
}
