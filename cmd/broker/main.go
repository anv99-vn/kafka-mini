package main

import (
	"log"
	"net"

	"github.com/fatih/color"
	"github.com/vna/kafka-mini/internal/admin"
	"github.com/vna/kafka-mini/internal/broker"
	pb "github.com/vna/kafka-mini/proto"
	"google.golang.org/grpc"
)

func main() {
	// Initialize Shared State
	manager := broker.NewTopicManager()

	// Start Admin HTTP Server in background
	httpServer := admin.NewHttpServer(manager)
	go func() {
		color.HiBlue("Admin UI listening at http://localhost:8080")
		if err := httpServer.Start(":8080"); err != nil {
			log.Fatalf("failed to start admin server: %v", err)
		}
	}()

	// Start gRPC Broker
	lis, err := net.Listen("tcp", ":9092")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	store, err := broker.NewMessageStore("./data")
	if err != nil {
		log.Fatalf("failed to initialize store: %v", err)
	}
	b := broker.NewBroker(manager, store)

	pb.RegisterProducerServiceServer(s, b)
	pb.RegisterConsumerServiceServer(s, b)
	pb.RegisterAdminServiceServer(s, b)

	color.Green("Broker gRPC listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
