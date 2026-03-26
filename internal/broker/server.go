package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/vna/kafka-mini/internal/raft"
	pb "github.com/vna/kafka-mini/proto"
	"google.golang.org/grpc/peer"
)

type Broker struct {
	pb.UnimplementedProducerServiceServer
	pb.UnimplementedConsumerServiceServer
	pb.UnimplementedAdminServiceServer

	mu sync.Mutex

	Manager *TopicManager
	Store   *MessageStore
	Raft    *raft.Server
	peerAddrs map[int32]string
	// index: topic -> partition -> entries
	index map[string]map[int32][]IndexEntry

	// groupOffsets: group -> topic_partition -> offset
	groupOffsets map[string]map[string]int64
	// subscriptions: group -> topics
	subscriptions map[string][]string

	// groupMembers: group -> member_id -> last_seen
	groupMembers map[string]map[string]time.Time
	// groupAssignment: group -> member_id -> partitions
	groupAssignment map[string]map[string][]*pb.TopicPartition
}

func NewBroker(manager *TopicManager, store *MessageStore, r *raft.Server, peerAddrs map[int32]string) *Broker {
	offsets, err := store.LoadOffsets()
	if err != nil {
		color.Red("Failed to load offsets: %v", err)
		offsets = make(map[string]map[string]int64)
	}

	b := &Broker{
		Manager:       manager,
		Store:         store,
		Raft:          r,
		peerAddrs:     peerAddrs,
		index:         make(map[string]map[int32][]IndexEntry),
		groupOffsets:  offsets,
		subscriptions: make(map[string][]string),
		groupMembers:  make(map[string]map[string]time.Time),
		groupAssignment: make(map[string]map[string][]*pb.TopicPartition),
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
	if b.Raft != nil {
		leaderId, _, isLeader := b.Raft.IsLeader()
		if !isLeader {
			if addr, ok := b.peerAddrs[leaderId]; ok {
				return nil, fmt.Errorf("not leader, hint: %s", addr)
			}
			return nil, fmt.Errorf("not leader")
		}
	}

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

	// 1. Identify Consumer
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("no peer info in context")
	}
	memberId := p.Addr.String()

	// 2. Update Heartbeat
	if b.groupMembers[req.GroupId] == nil {
		b.groupMembers[req.GroupId] = make(map[string]time.Time)
	}
	b.groupMembers[req.GroupId][memberId] = time.Now()

	// 3. Check for Timeout and Membership Changes
	rebalanceNeeded := false
	now := time.Now()
	for mId, lastSeen := range b.groupMembers[req.GroupId] {
		if now.Sub(lastSeen) > 10*time.Second {
			delete(b.groupMembers[req.GroupId], mId)
			rebalanceNeeded = true
		}
	}

	if _, assigned := b.groupAssignment[req.GroupId][memberId]; !assigned {
		rebalanceNeeded = true
	}

	if rebalanceNeeded {
		b.rebalance(req.GroupId)
	}

	// 4. Get Assigned Partitions
	assigned := b.groupAssignment[req.GroupId][memberId]
	if len(assigned) == 0 {
		return &pb.PollResponse{}, nil
	}

	if b.groupOffsets[req.GroupId] == nil {
		b.groupOffsets[req.GroupId] = make(map[string]int64)
	}

	deadline := time.Now().Add(time.Duration(req.TimeoutMs) * time.Millisecond)
	var allMsgs []*pb.Message

OuterLoop:
	for _, tp := range assigned {
		entries, ok := b.index[tp.Topic][tp.Partition]
		if !ok {
			continue
		}

		tpKey := fmt.Sprintf("%s_%d", tp.Topic, tp.Partition)
		currentOffset := b.groupOffsets[req.GroupId][tpKey]

		if currentOffset < int64(len(entries)) {
			count := 0
			// Fetch messages from current offset
			for i := currentOffset; i < int64(len(entries)); i++ {
				if time.Now().After(deadline) && len(allMsgs) > 0 {
					break OuterLoop
				}

				entry := entries[i]
				msg, err := b.Store.ReadAt(tp.Topic, tp.Partition, entry.Offset, entry.Length)
				if err == nil {
					msg.Offset = i
					msg.Topic = tp.Topic
					msg.Partition = tp.Partition
					allMsgs = append(allMsgs, msg)
					b.groupOffsets[req.GroupId][tpKey] = i + 1
					count++
				} else {
					color.Red("Failed to read message at topic=%s partition=%d offset=%d index=%d: %v", tp.Topic, tp.Partition, entry.Offset, i, err)
					// DO NOT increment the group offset so we can retry this message in the next poll
					break // Stop fetching for this partition in this poll
				}
			}
			if count > 0 {
				color.HiCyan("POLL_DELIVERY: topic=%s partition=%d count=%d member=%s", tp.Topic, tp.Partition, count, memberId)
			}
		}
	}

	return &pb.PollResponse{
		Messages: allMsgs,
	}, nil
}

func (b *Broker) rebalance(groupId string) {
	members := b.groupMembers[groupId]
	topics := b.subscriptions[groupId]

	var activeMembers []string
	for id := range members {
		activeMembers = append(activeMembers, id)
	}
	sort.Strings(activeMembers)

	var allPartitions []*pb.TopicPartition
	for _, topic := range topics {
		if t, ok := b.Manager.GetTopic(topic); ok {
			for p := int32(0); p < t.Partitions; p++ {
				allPartitions = append(allPartitions, &pb.TopicPartition{Topic: topic, Partition: p})
			}
		}
	}

	newAssignment := make(map[string][]*pb.TopicPartition)
	if len(activeMembers) > 0 {
		for i, tp := range allPartitions {
			mIdx := i % len(activeMembers)
			mId := activeMembers[mIdx]
			newAssignment[mId] = append(newAssignment[mId], tp)
		}
	}
	b.groupAssignment[groupId] = newAssignment
	color.HiMagenta("Rebalanced group %s: %d members, %d partitions distributed", groupId, len(activeMembers), len(allPartitions))
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

func (b *Broker) AddPeer(ctx context.Context, req *pb.AddPeerRequest) (*pb.Empty, error) {
	if b.Raft == nil {
		return nil, fmt.Errorf("raft not enabled")
	}
	_, _, isLeader := b.Raft.AddPeer(req.Id, req.Address)
	if !isLeader {
		return nil, fmt.Errorf("not leader")
	}
	b.mu.Lock()
	b.peerAddrs[req.Id] = req.Address
	b.mu.Unlock()
	return &pb.Empty{}, nil
}

func (b *Broker) RemovePeer(ctx context.Context, req *pb.RemovePeerRequest) (*pb.Empty, error) {
	if b.Raft == nil {
		return nil, fmt.Errorf("raft not enabled")
	}
	_, _, isLeader := b.Raft.RemovePeer(req.Id)
	if !isLeader {
		return nil, fmt.Errorf("not leader")
	}
	b.mu.Lock()
	delete(b.peerAddrs, req.Id)
	b.mu.Unlock()
	return &pb.Empty{}, nil
}

type GroupInfo struct {
	Name      string   `json:"name"`
	Topics    []string `json:"topics"`
	Members   int      `json:"members"`
	State     string   `json:"state"`
}

func (b *Broker) ListGroups() []GroupInfo {
	b.mu.Lock()
	defer b.mu.Unlock()

	res := make([]GroupInfo, 0, len(b.subscriptions))
	for name, topics := range b.subscriptions {
		members := len(b.groupMembers[name])
		state := "Stable"
		if members == 0 {
			state = "Empty"
		}
		res = append(res, GroupInfo{
			Name:    name,
			Topics:  topics,
			Members: members,
			State:   state,
		})
	}
	return res
}
