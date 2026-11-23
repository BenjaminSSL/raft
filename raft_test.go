package raft

import (
	"errors"
	"slices"
	"testing"
)

type cluster struct {
	t       *testing.T
	nodes   map[ID]*Node
	members []ID
	cut     map[ID]bool
}

func newCluster(t *testing.T, size int) *cluster {
	t.Helper()
	c := &cluster{t: t, nodes: make(map[ID]*Node), cut: make(map[ID]bool)}
	for i := 1; i <= size; i++ {
		c.members = append(c.members, ID(i))
	}
	for _, id := range c.members {
		c.nodes[id] = newTestNode(t, id, c.members, &MemoryStorage{})
	}
	return c
}

func newTestNode(t *testing.T, id ID, members []ID, storage Storage) *Node {
	t.Helper()
	n, err := New(Config{
		ID: id, Members: members, Storage: storage,
		ElectionTicks: 10, HeartbeatTicks: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func step(t *testing.T, n *Node, m Message) []Message {
	t.Helper()
	messages, err := n.Step(m)
	if err != nil {
		t.Fatal(err)
	}
	return messages
}

func (c *cluster) deliver(messages []Message) {
	c.t.Helper()
	for delivered := 0; len(messages) > 0; delivered++ {
		if delivered == 1000 {
			c.t.Fatal("message queue did not drain")
		}
		m := messages[0]
		messages = messages[1:]
		if !c.cut[m.From] && !c.cut[m.To] {
			messages = append(messages, step(c.t, c.nodes[m.To], m)...)
		}
	}
}

func (c *cluster) tick(id ID) {
	c.t.Helper()
	messages, err := c.nodes[id].Tick()
	if err != nil {
		c.t.Fatal(err)
	}
	c.deliver(messages)
}

func (c *cluster) elect(id ID) {
	c.t.Helper()
	for i := 0; i < 20 && c.nodes[id].Role() != Leader; i++ {
		c.tick(id)
	}
	if c.nodes[id].Role() != Leader {
		c.t.Fatalf("node %d did not become leader", id)
	}
}

func (c *cluster) propose(id ID, command string) int {
	c.t.Helper()
	index, messages, err := c.nodes[id].Propose(command)
	if err != nil {
		c.t.Fatal(err)
	}
	c.deliver(messages)
	return index
}

func assertCommands(t *testing.T, n *Node, want ...string) {
	t.Helper()
	var got []string
	for _, e := range n.Committed(0) {
		if !e.NoOp {
			got = append(got, e.Command)
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("node %d committed commands = %q, want %q", n.config.ID, got, want)
	}
}

func TestElectionAndReplication(t *testing.T) {
	c := newCluster(t, 3)
	c.elect(1)
	if _, _, err := c.nodes[2].Propose("wrong node"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower proposal returned %v", err)
	}
	index := c.propose(1, "set x=1")
	for _, id := range c.members {
		assertCommands(t, c.nodes[id], "set x=1")
		entries := c.nodes[id].Committed(index - 1)
		if len(entries) != 1 || entries[0].Command != "set x=1" {
			t.Fatalf("node %d returned wrong entries after index %d: %+v", id, index-1, entries)
		}
	}
	for i := 0; i < 40; i++ {
		for _, id := range c.members {
			c.tick(id)
		}
	}
	if c.nodes[1].Role() != Leader || c.nodes[1].Term() != 1 {
		t.Fatal("healthy leader lost leadership despite heartbeats")
	}
}

func TestPartitionRepairsConflictingLog(t *testing.T) {
	c := newCluster(t, 3)
	c.elect(1)
	c.propose(1, "common")
	c.cut[1] = true
	lost := c.propose(1, "isolated")
	assertCommands(t, c.nodes[1], "common")

	c.elect(2)
	c.propose(2, "majority")
	assertCommands(t, c.nodes[2], "common", "majority")
	assertCommands(t, c.nodes[3], "common", "majority")

	c.cut[1] = false
	for i := 0; i < 2; i++ {
		c.tick(2)
	}
	if c.nodes[1].Role() != Follower || c.nodes[1].Term() != c.nodes[2].Term() {
		t.Fatal("old leader did not step down")
	}
	assertCommands(t, c.nodes[1], "common", "majority")
	if entry := c.nodes[1].state.Log[lost-1]; !entry.NoOp || entry.Term != c.nodes[2].Term() {
		t.Fatalf("conflicting entry was not replaced: %+v", entry)
	}
}

func TestCandidateRequiresUniqueMajority(t *testing.T) {
	c := newCluster(t, 5)
	c.cut[1] = true
	n := c.nodes[1]
	for i := 0; i < 40 && n.Term() < 2; i++ {
		c.tick(1)
	}
	if n.Role() != Candidate || n.Term() != 2 {
		t.Fatal("isolated candidate did not retry the election")
	}
	reply := Message{Type: VoteResponse, From: 2, To: 1, Term: n.Term(), Success: true}
	step(t, n, reply)
	step(t, n, reply)
	if n.Role() != Candidate {
		t.Fatal("candidate counted a duplicate vote")
	}
	reply.From, reply.Term = 3, n.Term()-1
	step(t, n, reply)
	if n.Role() != Candidate {
		t.Fatal("candidate counted an old vote")
	}
	reply.Term = n.Term()
	step(t, n, reply)
	if n.Role() != Leader {
		t.Fatal("candidate did not win with a majority")
	}
}

func TestVoteSurvivesRestartAndRequiresUpToDateLog(t *testing.T) {
	members := []ID{1, 2, 3}
	store := &MemoryStorage{state: State{Term: 2, Log: []Entry{{Term: 2}}}}
	n := newTestNode(t, 1, members, store)
	vote := Message{Type: RequestVote, From: 2, To: 1, Term: 3, Index: 5, LogTerm: 1}
	if step(t, n, vote)[0].Success {
		t.Fatal("granted vote to candidate with a longer but older log")
	}
	vote.Index, vote.LogTerm = 0, 2
	if step(t, n, vote)[0].Success {
		t.Fatal("granted vote to candidate with a shorter log in the same last log term")
	}
	vote.Index = 1
	if !step(t, n, vote)[0].Success {
		t.Fatal("denied vote to candidate with matching log")
	}

	n = newTestNode(t, 1, members, store)
	vote.From = 3
	if step(t, n, vote)[0].Success {
		t.Fatal("granted a second vote in the same term after restart")
	}
	vote.Term++
	if !step(t, n, vote)[0].Success {
		t.Fatal("denied vote in a new term")
	}
}

func TestLeaderCommitsInheritedEntriesThroughCurrentTerm(t *testing.T) {
	members := []ID{1, 2, 3}
	store := &MemoryStorage{state: State{Term: 1, Log: []Entry{{Term: 1, Command: "inherited"}}}}
	n := newTestNode(t, 1, members, store)
	for n.Role() == Follower {
		if _, err := n.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	step(t, n, Message{Type: VoteResponse, From: 2, To: 1, Term: 2, Success: true})
	step(t, n, Message{Type: AppendResponse, From: 2, To: 1, Term: 2, Success: true, Match: 1})
	assertCommands(t, n)
	step(t, n, Message{Type: AppendResponse, From: 2, To: 1, Term: 2, Success: true, Index: 1, Match: 2})
	assertCommands(t, n, "inherited")
	// A delayed failure cannot roll back replication already confirmed by this peer.
	if messages := step(t, n, Message{Type: AppendResponse, From: 2, To: 1, Term: 2, Index: 2}); len(messages) != 0 {
		t.Fatal("retried entries already confirmed by the follower")
	}
}

func TestFollowerCommitsOnlyMatchedPrefix(t *testing.T) {
	store := &MemoryStorage{state: State{Term: 2, Log: []Entry{
		{Term: 1, Command: "common"}, {Term: 2, Command: "conflict"},
	}}}
	n := newTestNode(t, 1, []ID{1, 2, 3}, store)
	reply := step(t, n, Message{Type: AppendEntries, From: 2, To: 1, Term: 3, Index: 1, LogTerm: 1, Commit: 2})[0]
	if !reply.Success || reply.Match != 1 {
		t.Fatalf("unexpected append reply: %+v", reply)
	}
	assertCommands(t, n, "common")
}

func TestSingleNodeCommitsAndRecovers(t *testing.T) {
	c := newCluster(t, 1)
	c.elect(1)
	c.propose(1, "")
	c.propose(1, "saved")
	n := newTestNode(t, 1, c.members, c.nodes[1].config.Storage)
	if n.Role() != Follower || n.Term() != 1 {
		t.Fatal("restart lost term or retained volatile leadership")
	}
	assertCommands(t, n, "", "saved")
}

type failingStorage struct {
	MemoryStorage
	err error
}

func (s *failingStorage) Save(state State) error {
	if s.err != nil {
		return s.err
	}
	return s.MemoryStorage.Save(state)
}

func TestStorageFailureStopsNode(t *testing.T) {
	failure := errors.New("disk unavailable")
	store := &failingStorage{err: failure}
	n := newTestNode(t, 1, []ID{1, 2, 3}, store)
	m := Message{Type: RequestVote, From: 2, To: 1, Term: 1}
	messages, err := n.Step(m)
	if len(messages) != 0 || !errors.Is(err, failure) {
		t.Fatalf("failed persistence produced messages or wrong error: %v, %v", messages, err)
	}
	store.err = nil
	messages, err = n.Tick()
	if len(messages) != 0 || !errors.Is(err, failure) {
		t.Fatalf("node resumed after persistence failure: %v, %v", messages, err)
	}
}
