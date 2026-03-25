package broker

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"encoding/json"
	"github.com/fatih/color"
	pb "github.com/vna/kafka-mini/proto"
	"google.golang.org/protobuf/proto"
)

type IndexEntry struct {
	Offset int64
	Length int32
}

type MessageStore struct {
	locks   sync.Map // topic_partition (string) -> *sync.RWMutex
	BaseDir string
}

func NewMessageStore(baseDir string) (*MessageStore, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}
	return &MessageStore{BaseDir: baseDir}, nil
}

func (s *MessageStore) getLock(topic string, partition int32) *sync.RWMutex {
	key := fmt.Sprintf("%s_%d", topic, partition)
	l, _ := s.locks.LoadOrStore(key, &sync.RWMutex{})
	return l.(*sync.RWMutex)
}

func (s *MessageStore) getDataPath(topic string, partition int32) string {
	return filepath.Join(s.BaseDir, fmt.Sprintf("%s_%d.bin", topic, partition))
}

func (s *MessageStore) getIndexPath(topic string, partition int32) string {
	return filepath.Join(s.BaseDir, fmt.Sprintf("%s_%d.idx", topic, partition))
}

func (s *MessageStore) Append(topic string, partition int32, msg *pb.Message) (int64, int32, error) {
	mu := s.getLock(topic, partition)
	mu.Lock()
	defer mu.Unlock()

	// 1. Write Data to .bin file
	dataPath := s.getDataPath(topic, partition)
	df, err := os.OpenFile(dataPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, 0, err
	}
	defer df.Close()

	data, err := proto.Marshal(msg)
	if err != nil {
		return 0, 0, err
	}

	info, _ := df.Stat()
	offset := info.Size()

	if err := binary.Write(df, binary.LittleEndian, int32(len(data))); err != nil {
		return 0, 0, err
	}
	if _, err = df.Write(data); err != nil {
		return 0, 0, err
	}

	length := int32(len(data)) + 4

	// 2. Write Index to .idx file
	idxPath := s.getIndexPath(topic, partition)
	if err := s.appendIndex(idxPath, offset, length); err != nil {
		return 0, 0, fmt.Errorf("failed to write index: %v", err)
	}

	return offset, length, nil
}

func (s *MessageStore) appendIndex(path string, offset int64, length int32) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := binary.Write(f, binary.LittleEndian, offset); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, length); err != nil {
		return err
	}
	return nil
}

func (s *MessageStore) LoadIndex(topic string, partition int32) ([]IndexEntry, error) {
	mu := s.getLock(topic, partition)
	mu.RLock()
	defer mu.RUnlock()

	path := s.getIndexPath(topic, partition)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var entries []IndexEntry
	for {
		var offset int64
		var length int32
		if err := binary.Read(f, binary.LittleEndian, &offset); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if err := binary.Read(f, binary.LittleEndian, &length); err != nil {
			return nil, err
		}
		entries = append(entries, IndexEntry{Offset: offset, Length: length})
	}
	return entries, nil
}

func (s *MessageStore) ReadAt(topic string, partition int32, offset int64, length int32) (*pb.Message, error) {
	mu := s.getLock(topic, partition)
	mu.RLock()
	defer mu.RUnlock()

	path := s.getDataPath(topic, partition)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	frame := make([]byte, length)
	if _, err := f.ReadAt(frame, offset); err != nil {
		return nil, err
	}

	data := frame[4:]
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func (s *MessageStore) ReadAll(topic string, partition int32) ([]*pb.Message, error) {
	mu := s.getLock(topic, partition)
	mu.RLock()
	defer mu.RUnlock()

	path := s.getDataPath(topic, partition)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var messages []*pb.Message
	for {
		var length int32
		err := binary.Read(f, binary.LittleEndian, &length)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		data := make([]byte, length)
		if _, err := io.ReadFull(f, data); err != nil {
			return nil, err
		}

		msg := &pb.Message{}
		if err := proto.Unmarshal(data, msg); err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}

	return messages, nil
}

func (s *MessageStore) SaveOffsets(offsets map[string]map[string]int64) error {
	path := filepath.Join(s.BaseDir, "offsets.json")
	color.Yellow("Saving offsets to %s", path)
	data, err := json.MarshalIndent(offsets, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func (s *MessageStore) LoadOffsets() (map[string]map[string]int64, error) {
	path := filepath.Join(s.BaseDir, "offsets.json")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return make(map[string]map[string]int64), nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var offsets map[string]map[string]int64
	if err := json.Unmarshal(data, &offsets); err != nil {
		return nil, err
	}
	return offsets, nil
}
