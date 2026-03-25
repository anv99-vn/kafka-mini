package broker

import (
	"os"
	"testing"

	pb "github.com/vna/kafka-mini/proto"
)

func TestMessageStore_Indexing(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kafka-mini-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, _ := NewMessageStore(tmpDir)
	topic := "test-indexing"
	partition := int32(0)

	msgs := []*pb.Message{
		{Topic: topic, Value: []byte("msg1")},
		{Topic: topic, Value: []byte("msg2")},
		{Topic: topic, Value: []byte("msg3")},
	}

	expectedEntries := []IndexEntry{}
	for _, m := range msgs {
		offset, length, err := store.Append(topic, partition, m)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
		expectedEntries = append(expectedEntries, IndexEntry{Offset: offset, Length: length})
	}

	// Verify .idx file exists
	idxPath := store.getIndexPath(topic, partition)
	if _, err := os.Stat(idxPath); os.IsNotExist(err) {
		t.Errorf("Index file %s does not exist", idxPath)
	}

	// Verify LoadIndex
	loadedEntries, err := store.LoadIndex(topic, partition)
	if err != nil {
		t.Fatalf("LoadIndex failed: %v", err)
	}

	if len(loadedEntries) != len(expectedEntries) {
		t.Errorf("Expected %d entries, got %d", len(expectedEntries), len(loadedEntries))
	}

	for i, exp := range expectedEntries {
		if loadedEntries[i].Offset != exp.Offset || loadedEntries[i].Length != exp.Length {
			t.Errorf("Entry %d mismatch: expected %+v, got %+v", i, exp, loadedEntries[i])
		}
	}
}

func TestBroker_RecoveryFromIndex(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kafka-mini-recovery-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, _ := NewMessageStore(tmpDir)
	manager := NewTopicManager()
	topic := "recovery-topic"

	// 1. Manually create data and index as if the broker was running before
	msg := &pb.Message{Topic: topic, Value: []byte("recovery-test")}
	_, _, err = store.Append(topic, 0, msg)
	if err != nil {
		t.Fatalf("Initial append failed: %v", err)
	}

	// 2. Clear memory state and re-init broker
	b := NewBroker(manager, store, nil)

	// 3. Verify index was loaded
	b.mu.Lock()
	partitionIndex, ok := b.index[topic]
	var p0Entries []IndexEntry
	if ok {
		p0Entries = partitionIndex[0]
	}
	b.mu.Unlock()

	if !ok || len(p0Entries) == 0 {
		t.Fatalf("Topic %s partition 0 not found in broker index after recovery", topic)
	}
	if len(p0Entries) != 1 {
		t.Errorf("Expected 1 entry in recovered index, got %d", len(p0Entries))
	}

	// 4. Verify topic was registered in manager
	if _, ok := manager.GetTopic(topic); !ok {
		t.Errorf("Topic %s not auto-registered in manager during recovery", topic)
	}

	// 5. Verify data can be read via recovered index
	readMsg, err := store.ReadAt(topic, 0, p0Entries[0].Offset, p0Entries[0].Length)
	if err != nil {
		t.Fatalf("ReadAt failed using recovered index: %v", err)
	}
	if string(readMsg.Value) != string(msg.Value) {
		t.Errorf("Content mismatch: expected %s, got %s", msg.Value, readMsg.Value)
	}
}
