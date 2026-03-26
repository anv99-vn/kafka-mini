package main

import (
	"flag"
	"log"
	"net"
	"strings"

	"github.com/fatih/color"
	"github.com/vna/kafka-mini/internal/admin"
	"github.com/vna/kafka-mini/internal/broker"
	"github.com/vna/kafka-mini/internal/raft"
	pb "github.com/vna/kafka-mini/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	id := flag.Int("id", 0, "Broker ID / Raft Node ID")
	addr := flag.String("addr", ":9092", "Broker listen address")
	adminAddr := flag.String("admin", ":8080", "Admin UI listen address")
	peersFlag := flag.String("peers", "localhost:9092", "Comma-separated list of peer addresses")
	dataDir := flag.String("data", "./data", "Data directory")
	flag.Parse()

	// Initialize Shared State
	manager := broker.NewTopicManager()

	// Initialize Raft Peers
	peerAddrs := strings.Split(*peersFlag, ",")
	peers := make(map[int32]pb.RaftServiceClient)
	for i, peerAddr := range peerAddrs {
		if i == *id {
			continue // Self, no RPC client needed
		}
		conn, err := grpc.NewClient(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("failed to connect to peer %s: %v", peerAddr, err)
		}
		peers[int32(i)] = pb.NewRaftServiceClient(conn)
	}

	// Initialize Raft Server
	sm := &broker.BrokerStateMachine{Manager: manager}
	raftServer, err := raft.NewServer(int32(*id), peers, sm, *dataDir)
	if err != nil {
		log.Fatalf("failed to initialize raft: %v", err)
	}
	raftServer.Start()

	// Start gRPC Broker
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	store, err := broker.NewMessageStore(*dataDir)
	if err != nil {
		log.Fatalf("failed to initialize store: %v", err)
	}
	b := broker.NewBroker(manager, store, raftServer, peerAddrs)

	// Start Admin HTTP Server in background
	httpServer := admin.NewHttpServer(b)
	go func() {
		color.HiBlue("Admin UI listening at http://localhost%s", *adminAddr)
		if err := httpServer.Start(*adminAddr); err != nil {
			log.Fatalf("failed to start admin server: %v", err)
		}
	}()

	pb.RegisterProducerServiceServer(s, b)
	pb.RegisterConsumerServiceServer(s, b)
	pb.RegisterAdminServiceServer(s, b)
	pb.RegisterRaftServiceServer(s, raftServer) // Serve Raft RPCs on same port!

	color.Green("Broker gRPC listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
