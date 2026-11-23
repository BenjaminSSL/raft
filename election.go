package raft

func (n *Node) becomeFollower(term uint64) {
	if term > n.state.Term {
		n.state.Term = term
		n.state.Vote = 0
		n.dirty = true
	}
	n.role = Follower
	n.votes = nil
}

func (n *Node) startElection() []Message {
	n.role = Candidate
	n.state.Term++
	n.state.Vote = n.config.ID
	n.dirty = true
	n.votes = map[ID]bool{n.config.ID: true}
	n.resetElectionTimer()

	if len(n.votes) >= n.majority() {
		return n.becomeLeader()
	}
	last := n.lastIndex()
	var messages []Message
	for _, id := range n.config.Members {
		if id == n.config.ID {
			continue
		}
		m := n.message(id, RequestVote)
		m.Index, m.LogTerm = last, n.termAt(last)
		messages = append(messages, m)
	}
	return messages
}

func (n *Node) requestVote(m Message) []Message {
	reply := n.message(m.From, VoteResponse)
	last := n.lastIndex()
	lastTerm := n.termAt(last)
	upToDate := m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= last)
	canVote := n.state.Vote == 0 || n.state.Vote == m.From
	if m.Term == n.state.Term && canVote && upToDate {
		n.state.Vote = m.From
		n.dirty = true
		n.resetElectionTimer()
		reply.Success = true
	}
	return []Message{reply}
}

func (n *Node) receiveVote(m Message) []Message {
	if n.role != Candidate || m.Term != n.state.Term || !m.Success {
		return nil
	}
	n.votes[m.From] = true
	if len(n.votes) >= n.majority() {
		return n.becomeLeader()
	}
	return nil
}

func (n *Node) becomeLeader() []Message {
	n.role = Leader
	n.elapsed = 0
	n.votes = nil
	for _, peer := range n.peers {
		*peer = progress{next: n.lastIndex() + 1}
	}
	// Committing an entry from this term also commits the inherited prefix.
	n.appendEntry(Entry{Term: n.state.Term, NoOp: true})
	n.advanceCommit()
	return n.broadcastAppend()
}
