package raft

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/fatih/color"
	pb "github.com/vna/kafka-mini/proto"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type Server struct {
	pb.UnimplementedRaftServiceServer

	mu sync.Mutex

	id    int32
	peers map[int32]pb.RaftServiceClient // RPC clients to other nodes
	sm    StateMachine
	log   *Log

	state       State
	currentTerm int64
	votedFor    int32
	leaderId    int32

	commitIndex int64
	lastApplied int64

	// Leader state
	nextIndex  map[int32]int64
	matchIndex map[int32]int64

	electionResetEvent time.Time
	shutdownCh         chan struct{}
}

func NewServer(id int32, peers map[int32]pb.RaftServiceClient, sm StateMachine, baseDir string) (*Server, error) {
	raftLog, err := NewLog(baseDir)
	if err != nil {
		return nil, err
	}

	s := &Server{
		id:         id,
		peers:      peers,
		sm:         sm,
		log:        raftLog,
		state:      Follower,
		votedFor:   -1,
		leaderId:   -1,
		shutdownCh: make(chan struct{}),
	}

	// Initialize state from log
	if len(raftLog.entries) > 0 {
		s.currentTerm = raftLog.LastTerm()
	}

	return s, nil
}

func (s *Server) Start() {
	go s.runElectionTimer()
}

func (s *Server) Stop() {
	close(s.shutdownCh)
}

func (s *Server) runElectionTimer() {
	timeoutDuration := s.electionTimeout()
	s.mu.Lock()
	termStarted := s.currentTerm
	s.electionResetEvent = time.Now()
	s.mu.Unlock()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.shutdownCh:
			return
		case <-ticker.C:
			s.mu.Lock()
			if s.state != Leader && s.currentTerm == termStarted {
				if time.Since(s.electionResetEvent) >= timeoutDuration {
					s.startElection()
					s.mu.Unlock()
					return
				}
			} else {
				// State or term changed, restart timer for new state
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()
		}
	}
}

func (s *Server) electionTimeout() time.Duration {
	// 150ms to 300ms
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (s *Server) startElection() {
	s.state = Candidate
	s.currentTerm++
	savedCurrentTerm := s.currentTerm
	s.votedFor = s.id
	s.electionResetEvent = time.Now()
	color.Yellow("Node %d: becoming Candidate for term %d", s.id, savedCurrentTerm)

	votesReceived := 1
	var votesMu sync.Mutex

	if votesReceived*2 > len(s.peers)+1 {
		s.startLeader()
		return
	}

	for peerId, peer := range s.peers {
		go func(peer pb.RaftServiceClient, targetId int32) {
			s.mu.Lock()
			args := &pb.RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  s.id,
				LastLogIndex: s.log.LastIndex(),
				LastLogTerm:  s.log.LastTerm(),
			}
			s.mu.Unlock()

			reply, err := peer.RequestVote(context.Background(), args)
			if err != nil {
				return
			}

			s.mu.Lock()
			defer s.mu.Unlock()

			if s.state != Candidate || s.currentTerm != savedCurrentTerm {
				return
			}

			if reply.Term > savedCurrentTerm {
				s.becomeFollower(reply.Term)
				return
			}

			if reply.VoteGranted {
				votesMu.Lock()
				votesReceived++
				if votesReceived*2 > len(s.peers)+1 {
					s.startLeader()
				}
				votesMu.Unlock()
			}
		}(peer, peerId)
	}

	go s.runElectionTimer()
}

func (s *Server) becomeFollower(term int64) {
	color.HiBlack("Node %d: becoming Follower, term %d", s.id, term)
	s.state = Follower
	s.currentTerm = term
	s.votedFor = -1
	s.leaderId = -1
	s.electionResetEvent = time.Now()
	go s.runElectionTimer()
}

func (s *Server) startLeader() {
	color.HiGreen("Node %d: becoming Leader for term %d", s.id, s.currentTerm)
	s.state = Leader
	s.leaderId = s.id

	s.nextIndex = make(map[int32]int64)
	s.matchIndex = make(map[int32]int64)
	for peerId := range s.peers {
		s.nextIndex[peerId] = s.log.LastIndex() + 1
		s.matchIndex[peerId] = 0
	}
	// Also for self
	s.matchIndex[s.id] = s.log.LastIndex()
	s.nextIndex[s.id] = s.log.LastIndex() + 1

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			s.mu.Lock()
			if s.state != Leader {
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()

			s.sendHeartbeats()

			select {
			case <-s.shutdownCh:
				return
			case <-ticker.C:
			}
		}
	}()
}

func (s *Server) sendHeartbeats() {
	s.mu.Lock()
	savedCurrentTerm := s.currentTerm
	s.mu.Unlock()

	for peerId, peer := range s.peers {
		go func(peer pb.RaftServiceClient, targetId int32) {
			s.mu.Lock()
			prevLogIndex := s.nextIndex[targetId] - 1
			prevLogTerm := int64(0)
			if prevLogIndex >= 0 {
				if entry := s.log.Get(prevLogIndex); entry != nil {
					prevLogTerm = entry.Term
				}
			}
			entries := s.log.EntriesFrom(s.nextIndex[targetId])

			args := &pb.AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderId:     s.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: s.commitIndex,
			}
			s.mu.Unlock()

			reply, err := peer.AppendEntries(context.Background(), args)
			if err != nil {
				return
			}

			s.mu.Lock()
			defer s.mu.Unlock()

			if reply.Term > savedCurrentTerm {
				s.becomeFollower(reply.Term)
				return
			}

			if s.state != Leader || savedCurrentTerm != s.currentTerm {
				return
			}

			if reply.Success {
				if len(entries) > 0 {
					s.nextIndex[targetId] = entries[len(entries)-1].Index + 1
					s.matchIndex[targetId] = s.nextIndex[targetId] - 1
					
					// Check if we can commit
					savedCommitIndex := s.commitIndex
					for n := s.log.LastIndex(); n > s.commitIndex; n-- {
						entry := s.log.Get(n)
						if entry != nil && entry.Term == s.currentTerm {
							matchCount := 1
							for pId := range s.peers {
								if s.matchIndex[pId] >= n {
									matchCount++
								}
							}
							if matchCount*2 > len(s.peers)+1 {
								s.commitIndex = n
								break
							}
						}
					}
					if s.commitIndex > savedCommitIndex {
						s.applyCommitted()
					}
				}
			} else {
				s.nextIndex[targetId]--
			}
		}(peer, peerId)
	}
}

// Implements pb.RaftServiceServer
func (s *Server) RequestVote(ctx context.Context, args *pb.RequestVoteArgs) (*pb.RequestVoteReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if args.Term > s.currentTerm {
		s.becomeFollower(args.Term)
	}

	reply := &pb.RequestVoteReply{
		Term:        s.currentTerm,
		VoteGranted: false,
	}

	if args.Term == s.currentTerm && (s.votedFor == -1 || s.votedFor == args.CandidateId) {
		lastLogIndex := s.log.LastIndex()
		lastLogTerm := s.log.LastTerm()

		// Logs must be at least as up-to-date
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			reply.VoteGranted = true
			s.votedFor = args.CandidateId
			s.electionResetEvent = time.Now()
		}
	}

	return reply, nil
}

func (s *Server) AppendEntries(ctx context.Context, args *pb.AppendEntriesArgs) (*pb.AppendEntriesReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if args.Term > s.currentTerm {
		s.becomeFollower(args.Term)
	}

	reply := &pb.AppendEntriesReply{
		Term:    s.currentTerm,
		Success: false,
	}

	if args.Term < s.currentTerm {
		return reply, nil
	}

	s.leaderId = args.LeaderId
	s.electionResetEvent = time.Now()
	
	if s.state == Candidate {
		s.becomeFollower(args.Term)
	}

	// 1. Check prevLogIndex and prevLogTerm
	prevLogIndex := args.PrevLogIndex
	if prevLogIndex >= 0 {
		if prevLogIndex > s.log.LastIndex() {
			return reply, nil
		}
		if entry := s.log.Get(prevLogIndex); entry != nil && entry.Term != args.PrevLogTerm {
			return reply, nil
		}
	}

	// 2. Truncate if conflicting
	if len(args.Entries) > 0 {
		idx := args.Entries[0].Index
		if idx <= s.log.LastIndex() {
			s.log.Truncate(idx)
		}
		s.log.Append(args.Entries...)
	}

	// 3. Update commit index
	if args.LeaderCommit > s.commitIndex {
		s.commitIndex = args.LeaderCommit
		if s.log.LastIndex() < s.commitIndex {
			s.commitIndex = s.log.LastIndex()
		}
		// Apply committed to state machine
		s.applyCommitted()
	}

	reply.Success = true
	return reply, nil
}

// Propose appends a command to the log if this node is the leader.
func (s *Server) Propose(command []byte) (int64, int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != Leader {
		return -1, -1, false
	}

	index := s.log.LastIndex() + 1
	term := s.currentTerm

	entry := &pb.LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	}

	s.log.Append(entry)
	s.matchIndex[s.id] = index
	s.nextIndex[s.id] = index + 1

	go s.sendHeartbeats()

	return index, term, true
}

func (s *Server) IsLeader() (int32, int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leaderId, s.currentTerm, s.state == Leader
}

func (s *Server) applyCommitted() {
	if s.commitIndex > s.lastApplied {
		for i := s.lastApplied + 1; i <= s.commitIndex; i++ {
			entry := s.log.Get(i)
			if entry != nil && len(entry.Command) > 0 {
				s.sm.Apply(entry.Command)
			}
			s.lastApplied = i
		}
	}
}
