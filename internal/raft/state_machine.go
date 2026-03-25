package raft

type StateMachine interface {
	// Apply is invoked once a log entry is committed.
	// It should apply the command to the local state machine.
	Apply(command []byte) error
}
