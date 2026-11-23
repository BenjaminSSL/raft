package raft

import "slices"

func (n *Node) appendMessage(id ID) Message {
	prev := n.peers[id].next - 1
	m := n.message(id, AppendEntries)
	m.Index, m.LogTerm = prev, n.termAt(prev)
	m.Entries = slices.Clone(n.state.Log[prev:])
	m.Commit = n.state.Commit
	return m
}

func (n *Node) broadcastAppend() []Message {
	var messages []Message
	for _, id := range n.config.Members {
		if id != n.config.ID {
			messages = append(messages, n.appendMessage(id))
		}
	}
	return messages
}

func (n *Node) receiveAppend(m Message) []Message {
	reply := n.message(m.From, AppendResponse)
	reply.Index = m.Index
	if m.Term < n.state.Term {
		return []Message{reply}
	}
	if n.role != Follower {
		n.becomeFollower(m.Term)
	}
	n.resetElectionTimer()

	if m.Index < 0 || m.Index > n.lastIndex() || n.termAt(m.Index) != m.LogTerm {
		return []Message{reply}
	}

	// Keep the matching prefix. Replace the suffix only at the first conflict.
	for offset, entry := range m.Entries {
		index := m.Index + offset + 1
		if index <= n.lastIndex() && n.termAt(index) == entry.Term {
			continue
		}
		if index <= n.state.Commit {
			return []Message{reply}
		}
		n.state.Log = append(n.state.Log[:index-1], m.Entries[offset:]...)
		n.dirty = true
		break
	}

	reply.Success = true
	reply.Match = m.Index + len(m.Entries)
	// Entries after Match may still belong to a conflicting suffix.
	commit := min(m.Commit, reply.Match)
	if commit > n.state.Commit {
		n.setCommit(commit)
	}
	return []Message{reply}
}

func (n *Node) receiveAppendResponse(m Message) []Message {
	if n.role != Leader || m.Term != n.state.Term {
		return nil
	}
	peer := n.peers[m.From]
	if !m.Success {
		// Ignore failures for older probes and keep confirmed entries matched.
		if m.Index == peer.next-1 && peer.next > peer.match+1 {
			peer.next--
			return []Message{n.appendMessage(m.From)}
		}
		return nil
	}
	if m.Match < m.Index || m.Match > n.lastIndex() || m.Match <= peer.match {
		return nil
	}
	peer.match = m.Match
	peer.next = m.Match + 1
	if n.advanceCommit() {
		return n.broadcastAppend()
	}
	return nil
}

func (n *Node) advanceCommit() bool {
	for index := n.lastIndex(); index > n.state.Commit; index-- {
		// A leader counts replicas only for entries from its current term.
		if n.termAt(index) != n.state.Term {
			break
		}
		replicas := 1 // The leader's local copy.
		for _, peer := range n.peers {
			if peer.match >= index {
				replicas++
			}
		}
		if replicas >= n.majority() {
			n.setCommit(index)
			return true
		}
	}
	return false
}
