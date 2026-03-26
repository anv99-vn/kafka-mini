package broker

import (
	"context"
	"net"
	"os"
	"testing"

	pb "github.com/vna/kafka-mini/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestConsumerRebalance(t *testing.T) {
	// 1. Setup Broker
	tmpDir, _ := os.MkdirTemp("", "kafka-mini-rebalance-*")
	defer os.RemoveAll(tmpDir)

	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := lis.Addr().String()

	manager := NewTopicManager()
	store, _ := NewMessageStore(tmpDir)
	b := NewBroker(manager, store, nil, nil) // No Raft for simplicity

	s := grpc.NewServer()
	pb.RegisterConsumerServiceServer(s, b)
	pb.RegisterAdminServiceServer(s, b)
	go s.Serve(lis)
	defer s.Stop()

	// 2. Create Topic with 4 partitions
	topic := "rebalance-test"
	b.Manager.CreateTopic(topic, 4)
	for i := 0; i < 4; i++ {
		b.mu.Lock()
		if b.index[topic] == nil {
			b.index[topic] = make(map[int32][]IndexEntry)
		}
		b.index[topic][int32(i)] = []IndexEntry{} // Initialize partitions
		b.mu.Unlock()
	}

	groupId := "test-group"

	// 3. Start Consumer 1
	conn1, _ := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn1.Close()
	client1 := pb.NewConsumerServiceClient(conn1)

	_, err := client1.Subscribe(context.Background(), &pb.SubscribeRequest{GroupId: groupId, Topics: []string{topic}})
	if err != nil {
		t.Fatalf("Subscribe 1 failed: %v", err)
	}

	// First Poll triggers initial assignment (all 4 partitions to Consumer 1)
	_, _ = client1.Poll(context.Background(), &pb.PollRequest{GroupId: groupId, TimeoutMs: 100})
	
	b.mu.Lock()
	assign1 := len(b.groupAssignment[groupId])
	if assign1 != 1 {
		t.Errorf("Expected 1 assigned member, got %d", assign1)
	}
	b.mu.Unlock()

	// 4. Start Consumer 2
	conn2, _ := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn2.Close()
	client2 := pb.NewConsumerServiceClient(conn2)

	_, _ = client2.Subscribe(context.Background(), &pb.SubscribeRequest{GroupId: groupId, Topics: []string{topic}})
	
	// Poll 2 triggers rebalance (2 partitions each)
	_, _ = client2.Poll(context.Background(), &pb.PollRequest{GroupId: groupId, TimeoutMs: 100})

	b.mu.Lock()
	numMembers := len(b.groupMembers[groupId])
	if numMembers != 2 {
		t.Errorf("Expected 2 members in group, got %d", numMembers)
	}
	
	// Check assignments
	totalAssigned := 0
	for mId, tps := range b.groupAssignment[groupId] {
		t.Logf("Member %s assigned %d partitions", mId, len(tps))
		totalAssigned += len(tps)
		if len(tps) != 2 {
			t.Errorf("Expected 2 partitions for member %s, got %d", mId, len(tps))
		}
	}
	if totalAssigned != 4 {
		t.Errorf("Expected 4 partitions total, got %d", totalAssigned)
	}
	b.mu.Unlock()

	t.Log("Rebalance test passed!")
}
