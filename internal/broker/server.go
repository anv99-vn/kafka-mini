package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/vna/kafka-mini/internal/raft"
	pb "github.com/vna/kafka-mini/proto"
)

type Broker struct {
	pb.UnimplementedProducerServiceServer
	pb.UnimplementedConsumerServiceServer
	pb.UnimplementedAdminServiceServer

	mu sync.Mutex

	Manager *TopicManager
	Store   *MessageStore
	Raft    *raft.Server
	// index: topic -> partition -> entries
	index map[string]map[int32][]IndexEntry

	// groupOffsets: group -> topic_partition -> offset
	groupOffsets map[string]map[string]int64
	// subscriptions: group -> topics
	subscriptions map[string][]string
}

func NewBroker(manager *TopicManager, store *MessageStore, r *raft.Server) *Broker {
	offsets, err := store.LoadOffsets()
	if err != nil {
		color.Red("Failed to load offsets: %v", err)
		offsets = make(map[string]map[string]int64)
	}

	b := &Broker{
		Manager:       manager,
		Store:         store,
		Raft:          r,
		index:         make(map[string]map[int32][]IndexEntry),
		groupOffsets:  offsets,
		subscriptions: make(map[string][]string),
	}

	b.loadMessages()

	return b
}

func (b *Broker) loadMessages() {
	// Look for all .bin files to recover topics and partitions
	files, _ := filepath.Glob(filepath.Join(b.Store.BaseDir, "*.bin"))

	// Regex to extract topic and partition from "topic_partition.bin"
	re := regexp.MustCompile(`^(.+)_(\d+)\.bin$`)

	for _, f := range files {
		base := filepath.Base(f)
		matches := re.FindStringSubmatch(base)
		if len(matches) != 3 {
			continue
		}

		topic := matches[1]
		partition, _ := strconv.Atoi(matches[2])
		p32 := int32(partition)

		// Fast load from .idx file
		entries, err := b.Store.LoadIndex(topic, p32)
		if err != nil {
			color.Red("Failed to load index for topic %s partition %d: %v", topic, p32, err)
			continue
		}

		b.mu.Lock()
		if b.index[topic] == nil {
			b.index[topic] = make(map[int32][]IndexEntry)
		}
		b.index[topic][p32] = entries
		b.mu.Unlock()

		// Register topic if it doesn't exist
		if t, ok := b.Manager.GetTopic(topic); !ok {
			b.Manager.CreateTopic(topic, p32+1)
		} else if p32 >= t.Partitions {
			b.Manager.CreateTopic(topic, p32+1)
		}
	}
}

// Producer Service Handlers

func (b *Broker) Send(ctx context.Context, req *pb.SendRequest) (*pb.SendResponse, error) {
	color.Cyan("Received Send request for topic: %s with %d messages", req.Topic, len(req.Messages))

	numPartitions := int32(1)
	if t, ok := b.Manager.GetTopic(req.Topic); ok {
		numPartitions = t.Partitions
	}

	// Calculate partition based on key (FNV-1a)
	partition := getPartition(req.Key, numPartitions)
	color.HiYellow("Routing message for key '%s' to partition %d (total %d)", string(req.Key), partition, numPartitions)

	offsets := make([]int64, 0, len(req.Messages))
	for _, msg := range req.Messages {
		// Persist to disk (data + index) for the calculated partition
		fileOffset, length, err := b.Store.Append(req.Topic, partition, msg)
		if err != nil {
			return nil, err
		}

		b.mu.Lock()
		if b.index[req.Topic] == nil {
			b.index[req.Topic] = make(map[int32][]IndexEntry)
		}
		
		// The logical offset is the index in the current partition list
		logicalOffset := int64(len(b.index[req.Topic][partition]))
		msg.Offset = logicalOffset

		b.index[req.Topic][partition] = append(b.index[req.Topic][partition], IndexEntry{
			Offset: fileOffset,
			Length: length,
		})
		b.mu.Unlock()

		offsets = append(offsets, logicalOffset)
	}

	return &pb.SendResponse{
		Offsets:   offsets,
		Partition: partition,
	}, nil
}

// Consumer Service Handlers

func (b *Broker) Subscribe(ctx context.Context, req *pb.SubscribeRequest) (*pb.Empty, error) {
	color.Green("Received Subscribe request for group: %s, topics: %v", req.GroupId, req.Topics)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subscriptions[req.GroupId] = req.Topics
	return &pb.Empty{}, nil
}

func (b *Broker) Poll(ctx context.Context, req *pb.PollRequest) (*pb.PollResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	topics, ok := b.subscriptions[req.GroupId]
	if !ok {
		return &pb.PollResponse{}, nil
	}

	if b.groupOffsets[req.GroupId] == nil {
		b.groupOffsets[req.GroupId] = make(map[string]int64)
	}

	// Calculate deadline
	deadline := time.Now().Add(time.Duration(req.TimeoutMs) * time.Millisecond)
	var allMsgs []*pb.Message

OuterLoop:
	for _, topic := range topics {
		partitions, ok := b.index[topic]
		if !ok {
			continue
		}

		for p, entries := range partitions {
			tpKey := fmt.Sprintf("%s_%d", topic, p)
			currentOffset := b.groupOffsets[req.GroupId][tpKey]

			if currentOffset < int64(len(entries)) {
				// Fetch messages from current offset
				for i := currentOffset; i < int64(len(entries)); i++ {
					// Check timeout
					if time.Now().After(deadline) && len(allMsgs) > 0 {
						break OuterLoop
					}

					entry := entries[i]
					msg, err := b.Store.ReadAt(topic, p, entry.Offset, entry.Length)
					if err == nil {
						msg.Offset = i // Ensure we return the logical offset
						allMsgs = append(allMsgs, msg)
						// Partial advance: update offset after each successful fetch
						b.groupOffsets[req.GroupId][tpKey] = i + 1
					}
				}
			}
		}
	}

	if len(allMsgs) > 0 {
		// Offsets are updated in memory. They will be saved to disk only when Commit is called.
	}

	return &pb.PollResponse{
		Messages: allMsgs,
	}, nil
}

func (b *Broker) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.Empty, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.groupOffsets[req.GroupId] == nil {
		b.groupOffsets[req.GroupId] = make(map[string]int64)
	}

	for tp, offset := range req.Offsets {
		b.groupOffsets[req.GroupId][tp] = offset
		color.Yellow("Group %s committed offset %d for %s", req.GroupId, offset, tp)
	}

	if err := b.Store.SaveOffsets(b.groupOffsets); err != nil {
		color.Red("Failed to save offsets in Commit: %v", err)
	}
	return &pb.Empty{}, nil
}

func (b *Broker) Seek(ctx context.Context, req *pb.SeekRequest) (*pb.Empty, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.groupOffsets[req.GroupId] == nil {
		b.groupOffsets[req.GroupId] = make(map[string]int64)
	}

	tpKey := fmt.Sprintf("%s_%d", req.Partition.Topic, req.Partition.Partition)
	b.groupOffsets[req.GroupId][tpKey] = req.Offset
	color.Magenta("Group %s seeking to offset %d for %s", req.GroupId, req.Offset, tpKey)

	// In-memory seek only. Persist on Commit.
	return &pb.Empty{}, nil
}

// Admin Service Handlers

func (b *Broker) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	color.Blue("Received CreateTopic request: %s (partitions: %d)", req.Name, req.Partitions)
	if b.Raft != nil {
		cmd := RaftCommand{Op: "CreateTopic", Topic: req.Name, Partitions: req.Partitions}
		data, _ := json.Marshal(cmd)
		_, _, isLeader := b.Raft.Propose(data)
		if !isLeader {
			return &pb.CreateTopicResponse{Success: false, ErrorMessage: "not leader"}, nil
		}
	} else {
		// Fallback for non-raft mode
		b.Manager.CreateTopic(req.Name, req.Partitions)
	}

	return &pb.CreateTopicResponse{
		Success: true,
	}, nil
}

func (b *Broker) DeleteTopic(ctx context.Context, req *pb.DeleteTopicRequest) (*pb.Empty, error) {
	color.Red("Received DeleteTopic request: %s", req.Name)
	if b.Raft != nil {
		cmd := RaftCommand{Op: "DeleteTopic", Topic: req.Name}
		data, _ := json.Marshal(cmd)
		_, _, isLeader := b.Raft.Propose(data)
		if !isLeader {
			return &pb.Empty{}, fmt.Errorf("not leader")
		}
	} else {
		b.Manager.DeleteTopic(req.Name)
	}
	return &pb.Empty{}, nil
}

func (b *Broker) ListTopics(ctx context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	tms := b.Manager.ListTopics()
	names := make([]string, len(tms))
	for i, t := range tms {
		names[i] = t.Name
	}
	return &pb.ListTopicsResponse{
		Names: names,
	}, nil
}

func (b *Broker) DescribeTopic(ctx context.Context, req *pb.DescribeTopicRequest) (*pb.DescribeTopicResponse, error) {
	t, ok := b.Manager.GetTopic(req.Name)
	if !ok {
		return &pb.DescribeTopicResponse{}, nil
	}
	return &pb.DescribeTopicResponse{
		Metadata: t,
	}, nil
}
