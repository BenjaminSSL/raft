// Package raft implements Raft consensus for a fixed set of voting members.
// A caller drives each node with ticks and delivers the messages it returns.
package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"slices"
)

type ID uint64

type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

type MessageType uint8

const (
	RequestVote MessageType = iota + 1
	VoteResponse
	AppendEntries
	AppendResponse
)

// Entry contains a command or the no-op a leader adds at the start of its term.
type Entry struct {
	Term    uint64
	Command string
	NoOp    bool
}

// Message carries a Raft RPC or its reply. Log indexes start at 1.
// RequestVote uses Index and LogTerm for the candidate's last entry.
// AppendEntries uses them for the entry preceding Entries.
// AppendResponse echoes the preceding index and reports Match on success.
type Message struct {
	Type     MessageType
	From, To ID
	Term     uint64
	Index    int
	LogTerm  uint64
	Entries  []Entry
	Commit   int
	Success  bool
	Match    int
}

// State is the complete record saved by Storage. Vote zero means no vote.
type State struct {
	Term   uint64
	Vote   ID
	Log    []Entry
	Commit int
}

// Storage belongs to one node. Save must replace the record atomically and
// durably before returning nil. Load returns a zero State for a new node.
type Storage interface {
	Load() (State, error)
	Save(State) error
}

// MemoryStorage retains state across Node instances in the same process.
type MemoryStorage struct{ state State }

func (s *MemoryStorage) Load() (State, error) { return cloneState(s.state), nil }
func (s *MemoryStorage) Save(state State) error {
	s.state = cloneState(state)
	return nil
}

func cloneState(s State) State {
	s.Log = slices.Clone(s.Log)
	return s
}

type Config struct {
	ID             ID
	Members        []ID  // Includes ID. Zero is reserved.
	ElectionTicks  int   // Election timeout is randomized in [ElectionTicks, 2*ElectionTicks).
	HeartbeatTicks int   // Must be smaller than ElectionTicks.
	Seed           int64 // Combined with ID for reproducible timeouts.
	Storage        Storage
}

var ErrNotLeader = errors.New("raft: proposal requires a leader")

type progress struct {
	next  int // Next log index to send to this follower.
	match int // Highest index confirmed by this follower.
}

// Node is driven serially by its caller. A storage failure stops the node.
type Node struct {
	config Config
	random *rand.Rand
	state  State
	dirty  bool
	err    error

	role    Role
	elapsed int
	timeout int
	votes   map[ID]bool
	peers   map[ID]*progress
}

func New(config Config) (*Node, error) {
	if config.ID == 0 || config.Storage == nil || config.HeartbeatTicks <= 0 ||
		config.ElectionTicks <= config.HeartbeatTicks || config.ElectionTicks > int(^uint(0)>>1)/2 {
		return nil, errors.New("raft: invalid ID, storage, or timeouts")
	}
	config.Members = slices.Clone(config.Members)
	members := make(map[ID]bool)
	for _, id := range config.Members {
		if id == 0 || members[id] {
			return nil, errors.New("raft: member IDs must be unique and nonzero")
		}
		members[id] = true
	}
	if !members[config.ID] {
		return nil, errors.New("raft: members must include the local node")
	}
	state, err := config.Storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load state: %w", err)
	}
	if state.Commit < 0 || state.Commit > len(state.Log) ||
		(state.Vote != 0 && (!members[state.Vote] || state.Term == 0)) {
		return nil, errors.New("raft: invalid stored state")
	}
	var previousTerm uint64
	for _, entry := range state.Log {
		if entry.Term == 0 || entry.Term < previousTerm || entry.Term > state.Term {
			return nil, errors.New("raft: invalid stored log term")
		}
		previousTerm = entry.Term
	}
	n := &Node{
		config: config,
		random: rand.New(rand.NewSource(config.Seed ^ int64(config.ID))),
		state:  cloneState(state),
		peers:  make(map[ID]*progress),
	}
	for _, id := range config.Members {
		if id != config.ID {
			n.peers[id] = &progress{}
		}
	}
	n.resetElectionTimer()
	return n, nil
}

func (n *Node) Role() Role   { return n.role }
func (n *Node) Term() uint64 { return n.state.Term }

// Committed returns a copy of committed log entries after the supplied index.
// The caller applies them in order, skips no-ops, and tracks its applied index.
func (n *Node) Committed(after int) []Entry {
	if n.err != nil || after < 0 || after >= n.state.Commit {
		return nil
	}
	return slices.Clone(n.state.Log[after:n.state.Commit])
}

// Tick advances the local clock by one unit.
func (n *Node) Tick() ([]Message, error) {
	if n.err != nil {
		return nil, n.err
	}
	n.elapsed++
	if n.role == Leader {
		if n.elapsed >= n.config.HeartbeatTicks {
			n.elapsed = 0
			return n.broadcastAppend(), nil
		}
	} else if n.elapsed >= n.timeout {
		return n.finish(n.startElection())
	}
	return nil, nil
}

// Propose appends a command to the leader's log and returns its index.
// A successful proposal becomes safe to apply once it appears in Committed.
func (n *Node) Propose(command string) (int, []Message, error) {
	if n.err != nil {
		return 0, nil, n.err
	}
	if n.role != Leader {
		return 0, nil, ErrNotLeader
	}
	n.appendEntry(Entry{Term: n.state.Term, Command: command})
	n.advanceCommit()
	messages, err := n.finish(n.broadcastAppend())
	if err != nil {
		return 0, nil, err
	}
	return n.lastIndex(), messages, nil
}

// Step processes a message from another cluster member.
func (n *Node) Step(m Message) ([]Message, error) {
	if n.err != nil {
		return nil, n.err
	}
	if m.To != n.config.ID || n.peers[m.From] == nil ||
		m.Type < RequestVote || m.Type > AppendResponse || m.Term == 0 {
		return nil, nil
	}
	if m.Term > n.state.Term {
		n.becomeFollower(m.Term)
	}
	var messages []Message
	switch m.Type {
	case RequestVote:
		messages = n.requestVote(m)
	case VoteResponse:
		messages = n.receiveVote(m)
	case AppendEntries:
		messages = n.receiveAppend(m)
	case AppendResponse:
		messages = n.receiveAppendResponse(m)
	}
	return n.finish(messages)
}

// Persist changes before returning messages or exposing newly committed entries.
func (n *Node) finish(messages []Message) ([]Message, error) {
	if n.dirty {
		if err := n.config.Storage.Save(cloneState(n.state)); err != nil {
			n.err = fmt.Errorf("raft: save state: %w", err)
			return nil, n.err
		}
		n.dirty = false
	}
	return messages, nil
}

func (n *Node) lastIndex() int { return len(n.state.Log) }

func (n *Node) termAt(index int) uint64 {
	if index == 0 {
		return 0
	}
	return n.state.Log[index-1].Term
}

func (n *Node) majority() int { return len(n.config.Members)/2 + 1 }

func (n *Node) message(to ID, kind MessageType) Message {
	return Message{Type: kind, From: n.config.ID, To: to, Term: n.state.Term}
}

func (n *Node) appendEntry(entry Entry) {
	n.state.Log = append(n.state.Log, entry)
	n.dirty = true
}

func (n *Node) setCommit(index int) {
	n.state.Commit = index
	n.dirty = true
}

func (n *Node) resetElectionTimer() {
	n.elapsed = 0
	n.timeout = n.config.ElectionTicks + n.random.Intn(n.config.ElectionTicks)
}
