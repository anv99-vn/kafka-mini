package broker

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vna/kafka-mini/internal/raft"
	"github.com/vna/kafka-mini/pkg/client"
	pb "github.com/vna/kafka-mini/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestRaftReplication(t *testing.T) {
	numNodes := 3
	tmpDirs := make([]string, numNodes)
	listeners := make([]net.Listener, numNodes)
	peerAddrs := make([]string, numNodes)
	var err error

	// 1. Setup Phase: Create listeners to get ports and allocate temp directories
	for i := 0; i < numNodes; i++ {
		tmpDirs[i], err = os.MkdirTemp("", fmt.Sprintf("kafka-mini-cluster-%d-*", i))
		if err != nil {
			t.Fatalf("Failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDirs[i])

		listeners[i], err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Failed to listen: %v", err)
		}
		peerAddrs[i] = listeners[i].Addr().String()
	}

	// 2. Initialize gRPC clients
	clients := make([]map[int32]pb.RaftServiceClient, numNodes)
	for i := 0; i < numNodes; i++ {
		clients[i] = make(map[int32]pb.RaftServiceClient)
		for j := 0; j < numNodes; j++ {
			if i == j {
				continue
			}
			conn, err := grpc.NewClient(peerAddrs[j], grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatalf("Failed to dial peer %d: %v", j, err)
			}
			clients[i][int32(j)] = pb.NewRaftServiceClient(conn)
			defer conn.Close()
		}
	}

	brokers := make([]*Broker, numNodes)
	servers := make([]*grpc.Server, numNodes)
	raftServers := make([]*raft.Server, numNodes)

	// 3. Start nodes
	for i := 0; i < numNodes; i++ {
		manager := NewTopicManager()
		store, _ := NewMessageStore(tmpDirs[i])
		sm := &BrokerStateMachine{Manager: manager}

		raftServer, err := raft.NewServer(int32(i), clients[i], sm, tmpDirs[i])
		if err != nil {
			t.Fatalf("Failed to create raft server %d: %v", i, err)
		}
		
		b := NewBroker(manager, store, raftServer, peerAddrs)
		brokers[i] = b
		raftServers[i] = raftServer

		servers[i] = grpc.NewServer()
		pb.RegisterAdminServiceServer(servers[i], b)
		pb.RegisterRaftServiceServer(servers[i], raftServer)

		raftServer.Start()
		
		go func(idx int) {
			_ = servers[idx].Serve(listeners[idx])
		}(i)
	}

	// Wait for leader election
	t.Log("Waiting for leader election...")
	time.Sleep(3 * time.Second)

	// 4. Test Replication: Create Topic
	topicName := "distributed-test-topic"
	t.Log("Sending CreateTopic request to node 0...")
	
	req := &pb.CreateTopicRequest{Name: topicName, Partitions: 3}
	success := false

	// Since Node 0 might not be the leader, we try all nodes until it succeeds.
	for i := 0; i < numNodes; i++ {
		res, err := brokers[i].CreateTopic(context.Background(), req)
		if err == nil && res.Success {
			success = true
			t.Logf("Created topic successfully via node %d (Leader)", i)
			break
		}
	}

	if !success {
		t.Fatalf("Failed to create topic on any node. No leader?")
	}

	// Wait for replication
	t.Log("Waiting for replication...")
	time.Sleep(1 * time.Second)

	// 5. Verify the data exists on all nodes
	for i := 0; i < numNodes; i++ {
		_, ok := brokers[i].Manager.GetTopic(topicName)
		if !ok {
			t.Errorf("Node %d is missing the topic '%s'", i, topicName)
		} else {
			t.Logf("Node %d successfully replicated topic '%s'", i, topicName)
		}

		// Also verify via log files
		logFile := filepath.Join(tmpDirs[i], "raft_log.json")
		if _, err := os.Stat(logFile); os.IsNotExist(err) {
			t.Errorf("Node %d is missing raft_log.json", i)
		}
	}

	// 6. Test Deletion
	t.Log("Sending DeleteTopic request...")
	delReq := &pb.DeleteTopicRequest{Name: topicName}
	success = false

	for i := 0; i < numNodes; i++ {
		_, err := brokers[i].DeleteTopic(context.Background(), delReq)
		if err == nil {
			success = true
			t.Logf("Deleted topic successfully via node %d", i)
			break
		}
	}

	if !success {
		t.Fatalf("Failed to delete topic on any node.")
	}

	time.Sleep(1 * time.Second)

	// Verify deletion on all nodes
	for i := 0; i < numNodes; i++ {
		_, ok := brokers[i].Manager.GetTopic(topicName)
		if ok {
			t.Errorf("Node %d still has the topic '%s' after deletion", i, topicName)
		}
	}

	// 7. Teardown
	for i := 0; i < numNodes; i++ {
		raftServers[i].Stop()
		servers[i].Stop()
	}
}

func TestProduceConsumeAfterLeaderFailure(t *testing.T) {
	numNodes := 3
	tmpDirs := make([]string, numNodes)
	listeners := make([]net.Listener, numNodes)
	peerAddrs := make([]string, numNodes)
	var err error

	for i := 0; i < numNodes; i++ {
		tmpDirs[i], err = os.MkdirTemp("", fmt.Sprintf("kafka-mini-cluster-fail-%d-*", i))
		if err != nil {
			t.Fatalf("Failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDirs[i])

		listeners[i], err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Failed to listen: %v", err)
		}
		peerAddrs[i] = listeners[i].Addr().String()
	}

	clients := make([]map[int32]pb.RaftServiceClient, numNodes)
	for i := 0; i < numNodes; i++ {
		clients[i] = make(map[int32]pb.RaftServiceClient)
		for j := 0; j < numNodes; j++ {
			if i == j {
				continue
			}
			conn, err := grpc.NewClient(peerAddrs[j], grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatalf("Failed to dial peer %d: %v", j, err)
			}
			clients[i][int32(j)] = pb.NewRaftServiceClient(conn)
			defer conn.Close()
		}
	}

	brokers := make([]*Broker, numNodes)
	servers := make([]*grpc.Server, numNodes)
	raftServers := make([]*raft.Server, numNodes)

	for i := 0; i < numNodes; i++ {
		manager := NewTopicManager()
		store, _ := NewMessageStore(tmpDirs[i])
		sm := &BrokerStateMachine{Manager: manager}

		raftServer, err := raft.NewServer(int32(i), clients[i], sm, tmpDirs[i])
		if err != nil {
			t.Fatalf("Failed to create raft server %d: %v", i, err)
		}

		b := NewBroker(manager, store, raftServer, peerAddrs)
		brokers[i] = b
		raftServers[i] = raftServer

		servers[i] = grpc.NewServer()
		pb.RegisterAdminServiceServer(servers[i], b)
		pb.RegisterProducerServiceServer(servers[i], b)
		pb.RegisterConsumerServiceServer(servers[i], b)
		pb.RegisterRaftServiceServer(servers[i], raftServer)

		raftServer.Start()

		go func(idx int) {
			_ = servers[idx].Serve(listeners[idx])
		}(i)
	}

	t.Log("Waiting for initial leader election...")
	time.Sleep(3 * time.Second)

	// 1. Create a topic via any node
	topicName := "failover-topic"
	req := &pb.CreateTopicRequest{Name: topicName, Partitions: 1}
	leaderIdx := -1
	for i := 0; i < numNodes; i++ {
		res, err := brokers[i].CreateTopic(context.Background(), req)
		if err == nil && res.Success {
			leaderIdx = i
			t.Logf("Created topic successfully via node %d (Leader)", i)
			break
		}
	}

	if leaderIdx == -1 {
		t.Fatalf("Failed to create topic. No leader elected?")
	}

	time.Sleep(1 * time.Second)

	// 2. Kill the leader!
	t.Logf("Killing Leader node %d...", leaderIdx)
	raftServers[leaderIdx].Stop()
	servers[leaderIdx].Stop()

	// Wait for a new leader to be elected
	t.Log("Waiting for new leader election...")
	time.Sleep(3 * time.Second)

	// 3. Find the new leader and produce messages
	newLeaderIdx := -1
	for i := 0; i < numNodes; i++ {
		if i == leaderIdx {
			continue // skip dead node
		}
		// Probe new leader by creating a test topic
		res, err := brokers[i].CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: "probe-topic", Partitions: 1})
		if err == nil && res.Success {
			newLeaderIdx = i
			break
		}
	}

	if newLeaderIdx == -1 {
		t.Fatalf("Failed to elect new leader after failure.")
	}

	t.Logf("Node %d is the new leader. Proceeding to Produce/Consume on alive node.", newLeaderIdx)

	// We can produce to any alive node (even if it's not the Raft leader, because our produce API is local for now)
	// But let's produce to the new leader.
	aliveNode := brokers[newLeaderIdx]

	sendReq := &pb.SendRequest{
		Topic: topicName,
		Key:   []byte("test_key"),
		Messages: []*pb.Message{
			{Topic: topicName, Value: []byte("msg1")},
			{Topic: topicName, Value: []byte("msg2")},
		},
	}

	t.Logf("Producing messages to node %d...", newLeaderIdx)
	sendRes, err := aliveNode.Send(context.Background(), sendReq)
	if err != nil {
		t.Fatalf("Produce failed: %v", err)
	}
	if len(sendRes.Offsets) != 2 {
		t.Errorf("Expected 2 offsets, got %d", len(sendRes.Offsets))
	}

	// 4. Consume from the same node
	t.Logf("Consuming messages from node %d...", newLeaderIdx)
	// Must subscribe first
	_, err = aliveNode.Subscribe(context.Background(), &pb.SubscribeRequest{
		GroupId: "test-group",
		Topics:  []string{topicName},
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	pollReq := &pb.PollRequest{
		GroupId:   "test-group",
		TimeoutMs: 1000,
	}

	pollRes, err := aliveNode.Poll(context.Background(), pollReq)
	if err != nil {
		t.Fatalf("Poll failed: %v", err)
	}

	if len(pollRes.Messages) != 2 {
		t.Errorf("Expected 2 messages, got %d", len(pollRes.Messages))
	} else {
		if string(pollRes.Messages[0].Value) != "msg1" || string(pollRes.Messages[1].Value) != "msg2" {
			t.Errorf("Message content mismatch")
		} else {
			t.Log("Successfully consumed messages after leader failure!")
		}
	}

	// Teardown remaining nodes
	for i := 0; i < numNodes; i++ {
		if i != leaderIdx {
			raftServers[i].Stop()
			servers[i].Stop()
		}
	}
}

func TestClientProducerFailover(t *testing.T) {
	numNodes := 3
	tmpDirs := make([]string, numNodes)
	listeners := make([]net.Listener, numNodes)
	peerAddrs := make([]string, numNodes)
	var err error

	for i := 0; i < numNodes; i++ {
		tmpDirs[i], err = os.MkdirTemp("", fmt.Sprintf("kafka-mini-cluster-client-fail-%d-*", i))
		if err != nil {
			t.Fatalf("Failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDirs[i])

		listeners[i], err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Failed to listen: %v", err)
		}
		peerAddrs[i] = listeners[i].Addr().String()
	}

	clients := make([]map[int32]pb.RaftServiceClient, numNodes)
	for i := 0; i < numNodes; i++ {
		clients[i] = make(map[int32]pb.RaftServiceClient)
		for j := 0; j < numNodes; j++ {
			if i == j {
				continue
			}
			conn, err := grpc.NewClient(peerAddrs[j], grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatalf("Failed to dial peer %d: %v", j, err)
			}
			clients[i][int32(j)] = pb.NewRaftServiceClient(conn)
			defer conn.Close()
		}
	}

	brokers := make([]*Broker, numNodes)
	servers := make([]*grpc.Server, numNodes)
	raftServers := make([]*raft.Server, numNodes)

	for i := 0; i < numNodes; i++ {
		manager := NewTopicManager()
		store, _ := NewMessageStore(tmpDirs[i])
		sm := &BrokerStateMachine{Manager: manager}

		raftServer, err := raft.NewServer(int32(i), clients[i], sm, tmpDirs[i])
		if err != nil {
			t.Fatalf("Failed to create raft server %d: %v", i, err)
		}

		b := NewBroker(manager, store, raftServer, peerAddrs)
		brokers[i] = b
		raftServers[i] = raftServer

		servers[i] = grpc.NewServer()
		pb.RegisterAdminServiceServer(servers[i], b)
		pb.RegisterProducerServiceServer(servers[i], b)
		pb.RegisterConsumerServiceServer(servers[i], b)
		pb.RegisterRaftServiceServer(servers[i], raftServer)

		raftServer.Start()

		go func(idx int) {
			_ = servers[idx].Serve(listeners[idx])
		}(i)
	}

	t.Log("Waiting for initial leader election...")
	time.Sleep(3 * time.Second)

	// Create a topic 
	topicName := "producer-failover-topic"
	req := &pb.CreateTopicRequest{Name: topicName, Partitions: 1}
	leaderIdx := -1
	for i := 0; i < numNodes; i++ {
		res, err := brokers[i].CreateTopic(context.Background(), req)
		if err == nil && res.Success {
			leaderIdx = i
			break
		}
	}
	if leaderIdx == -1 {
		t.Fatalf("No leader found for creating topic")
	}

	time.Sleep(1 * time.Second) // wait for replication

	// Now initialize the Producer client with ALL addresses
	allPeers := strings.Join(peerAddrs, ",")
	t.Logf("Initializing Producer with bootstrap servers: %s", allPeers)
	
	p, err := client.NewProducer(allPeers)
	if err != nil {
		t.Fatalf("Failed to create Producer: %v", err)
	}
	defer p.Close()

	// 2. Kill the leader first to make Producer fall back!
	t.Logf("Killing Leader %d (address %s) to trigger connection failure...", leaderIdx, peerAddrs[leaderIdx])
	raftServers[leaderIdx].Stop()
	servers[leaderIdx].Stop()

	// Wait a moment for new leader
	time.Sleep(3 * time.Second)

	// 3. Send message. The producer will attempt to use the first address (which might be dead),
	// catch the error, reconnect to the next alive node, and succeed.
	t.Log("Producer sending message...")
	_, err = p.Send(topicName, []byte("key"), []byte("payload"))
	if err != nil {
		t.Fatalf("Producer failed to failover and send: %v", err)
	}

	t.Log("SUCCESS: Producer successfully sent message by finding a new alive node!")

	for i := 0; i < numNodes; i++ {
		if i != leaderIdx {
			raftServers[i].Stop()
			servers[i].Stop()
		}
	}
}
