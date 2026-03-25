package broker

import (
	"encoding/json"
	"github.com/vna/kafka-mini/internal/raft"
	"github.com/fatih/color"
)

type RaftCommand struct {
	Op         string `json:"op"`
	Topic      string `json:"topic"`
	Partitions int32  `json:"partitions"`
}

type BrokerStateMachine struct {
	Manager *TopicManager
}

// Compile time check
var _ raft.StateMachine = (*BrokerStateMachine)(nil)

func (sm *BrokerStateMachine) Apply(data []byte) error {
	var cmd RaftCommand
	if err := json.Unmarshal(data, &cmd); err != nil {
		return err
	}
	color.HiCyan("Raft applying command: %s %s", cmd.Op, cmd.Topic)
	if cmd.Op == "CreateTopic" {
		sm.Manager.CreateTopic(cmd.Topic, cmd.Partitions)
	} else if cmd.Op == "DeleteTopic" {
		sm.Manager.DeleteTopic(cmd.Topic)
	}
	return nil
}
