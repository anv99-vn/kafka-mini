package broker

import (
	"sync"
	pb "github.com/vna/kafka-mini/proto"
)

type TopicManager struct {
	mu     sync.RWMutex
	topics map[string]*pb.TopicMetadata
}

func NewTopicManager() *TopicManager {
	return &TopicManager{
		topics: make(map[string]*pb.TopicMetadata),
	}
}

func (m *TopicManager) CreateTopic(name string, partitions int32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.topics[name] = &pb.TopicMetadata{
		Name:       name,
		Partitions: partitions,
	}
}

func (m *TopicManager) DeleteTopic(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.topics, name)
}

func (m *TopicManager) ListTopics() []*pb.TopicMetadata {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := make([]*pb.TopicMetadata, 0, len(m.topics))
	for _, t := range m.topics {
		res = append(res, t)
	}
	return res
}

func (m *TopicManager) GetTopic(name string) (*pb.TopicMetadata, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.topics[name]
	return t, ok
}
