package raft

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	pb "github.com/vna/kafka-mini/proto"
)

type Log struct {
	mu      sync.RWMutex
	baseDir string
	entries []*pb.LogEntry
}

func NewLog(baseDir string) (*Log, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}
	l := &Log{
		baseDir: baseDir,
		entries: make([]*pb.LogEntry, 0),
	}
	// A dummy entry at index 0 to simplify logic
	l.entries = append(l.entries, &pb.LogEntry{Term: 0, Index: 0})
	
	if err := l.load(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Log) load() error {
	path := filepath.Join(l.baseDir, "raft_log.json")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	// Exclude the dummy entry, replace with saved entries
	var saved []*pb.LogEntry
	if err := json.Unmarshal(data, &saved); err != nil {
		return err
	}
	l.entries = saved
	return nil
}

func (l *Log) save() error {
	path := filepath.Join(l.baseDir, "raft_log.json")
	data, err := json.MarshalIndent(l.entries, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func (l *Log) Append(entries ...*pb.LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entries...)
	return l.save()
}

func (l *Log) Get(index int64) *pb.LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index < 0 || index >= int64(len(l.entries)) {
		return nil
	}
	return l.entries[index]
}

func (l *Log) LastIndex() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return int64(len(l.entries) - 1)
}

func (l *Log) LastTerm() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

func (l *Log) Truncate(index int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index >= 0 && index < int64(len(l.entries)) {
		l.entries = l.entries[:index]
		return l.save()
	}
	return nil
}

func (l *Log) EntriesFrom(index int64) []*pb.LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index < 0 || index >= int64(len(l.entries)) {
		return nil
	}
	return l.entries[index:]
}
