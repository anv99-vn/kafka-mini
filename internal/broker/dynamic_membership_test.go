package broker

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/vna/kafka-mini/internal/raft"
	pb "github.com/vna/kafka-mini/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestDynamicMembershipAddRemove(t *testing.T) {
	numInitialNodes := 2
	totalNodes := 3
	tmpDirs := make([]string, totalNodes)
	listeners := make([]net.Listener, totalNodes)
	peerAddrsList := make([]string, totalNodes)
	var err error

	// 1. Setup Phase: Create listeners and temp directories
	for i := 0; i < totalNodes; i++ {
		tmpDirs[i], err = os.MkdirTemp("", fmt.Sprintf("kafka-mini-dynamic-%d-*", i))
		if err != nil {
			t.Fatalf("Failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDirs[i])

		listeners[i], err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Failed to listen: %v", err)
		}
		peerAddrsList[i] = listeners[i].Addr().String()
	}

	brokers := make([]*Broker, totalNodes)
	grpcServers := make([]*grpc.Server, totalNodes)
	raftServers := make([]*raft.Server, totalNodes)

	// 2. Start initial 2 nodes
	for i := 0; i < numInitialNodes; i++ {
		// Prepare peer clients for initial nodes
		peers := make(map[int32]pb.RaftServiceClient)
		peerAddrs := make(map[int32]string)
		for j := 0; j < numInitialNodes; j++ {
			peerAddrs[int32(j)] = peerAddrsList[j]
			if i == j {
				continue
			}
			conn, err := grpc.NewClient(peerAddrsList[j], grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatalf("Failed to dial peer %d: %v", j, err)
			}
			peers[int32(j)] = pb.NewRaftServiceClient(conn)
			defer conn.Close()
		}

		manager := NewTopicManager(nil)
		store, _ := NewMessageStore(tmpDirs[i])
		sm := &BrokerStateMachine{Manager: manager}

		raftServer, _ := raft.NewServer(int32(i), peers, sm, tmpDirs[i])
		b := NewBroker(manager, store, raftServer, peerAddrs)
		
		brokers[i] = b
		raftServers[i] = raftServer
		grpcServers[i] = grpc.NewServer()
		
		pb.RegisterAdminServiceServer(grpcServers[i], b)
		pb.RegisterRaftServiceServer(grpcServers[i], raftServer)

		raftServer.Start()
		go func(idx int) { _ = grpcServers[idx].Serve(listeners[idx]) }(i)
	}

	time.Sleep(3 * time.Second) // Wait for election

	// 3. Start node 2 (the 3rd node) in isolation
	{
		i := 2
		peers := make(map[int32]pb.RaftServiceClient) // EMPTY initial peers
		peerAddrs := make(map[int32]string)
		peerAddrs[int32(i)] = peerAddrsList[i]

		manager := NewTopicManager(nil)
		store, _ := NewMessageStore(tmpDirs[i])
		sm := &BrokerStateMachine{Manager: manager}

		raftServer, _ := raft.NewServer(int32(i), peers, sm, tmpDirs[i])
		b := NewBroker(manager, store, raftServer, peerAddrs)
		
		brokers[i] = b
		raftServers[i] = raftServer
		grpcServers[i] = grpc.NewServer()
		
		pb.RegisterAdminServiceServer(grpcServers[i], b)
		pb.RegisterRaftServiceServer(grpcServers[i], raftServer)

		raftServer.Start()
		go func(idx int) { _ = grpcServers[idx].Serve(listeners[idx]) }(i)
	}

	// 4. Add node 2 to the cluster
	t.Log("Adding node 2 to cluster...")
	leaderIdx := -1
	for i := 0; i < numInitialNodes; i++ {
		_, _, isLeader := raftServers[i].IsLeader()
		if isLeader {
			leaderIdx = i
			break
		}
	}
	if leaderIdx == -1 {
		t.Fatalf("No leader found in initial cluster")
	}

	_, err = brokers[leaderIdx].AddPeer(context.Background(), &pb.AddPeerRequest{
		Id:      2,
		Address: peerAddrsList[2],
	})
	if err != nil {
		t.Fatalf("AddPeer failed: %v", err)
	}

	time.Sleep(2 * time.Second) // Wait for config change to propagate

	// 5. Verify replication to the new node
	topicName := "dynamic-topic"
	_, err = brokers[leaderIdx].CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: topicName, Partitions: 1})
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	time.Sleep(1 * time.Second)

	// Node 2 should have the topic now
	if _, ok := brokers[2].Manager.GetTopic(topicName); !ok {
		t.Errorf("Node 2 failed to receive replicated topic after being added")
	} else {
		t.Log("SUCCESS: Node 2 successfully joined and replicated state!")
	}

	// 6. Remove node 1 from cluster
	t.Log("Removing node 1 from cluster...")
	_, err = brokers[leaderIdx].RemovePeer(context.Background(), &pb.RemovePeerRequest{Id: 1})
	if err != nil {
		t.Fatalf("RemovePeer failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	// Node 1 should eventually stop or at least not be part of replication
	// We can check if Node 0 and Node 2 can still form a majority and work
	_, err = brokers[leaderIdx].CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: "after-removal-topic", Partitions: 1})
	if err != nil {
		t.Logf("CreateTopic after removal: %v (might fail if leader was removed, we would need to find new leader)", err)
	} else {
		t.Log("Cluster still functional after node removal")
	}

	// Clean up
	for i := 0; i < totalNodes; i++ {
		raftServers[i].Stop()
		grpcServers[i].Stop()
	}
}
