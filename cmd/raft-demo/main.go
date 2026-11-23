package main

import (
	"fmt"
	"log"

	"raft"
)

func main() {
	members := []raft.ID{1, 2, 3}
	nodes := make(map[raft.ID]*raft.Node)
	for _, id := range members {
		node, err := raft.New(raft.Config{
			ID: id, Members: members, ElectionTicks: 10, HeartbeatTicks: 2,
			Storage: &raft.MemoryStorage{},
		})
		if err != nil {
			log.Fatal(err)
		}
		nodes[id] = node
	}

	var leader raft.ID
	for tick := 1; tick <= 100 && leader == 0; tick++ {
		for _, id := range members {
			messages, err := nodes[id].Tick()
			if err != nil {
				log.Fatal(err)
			}
			deliver(nodes, messages)
			if nodes[id].Role() == raft.Leader {
				leader = id
				fmt.Printf("Tick %d: elected node %d in term %d\n", tick, id, nodes[id].Term())
				break
			}
		}
	}
	if leader == 0 {
		log.Fatal("no leader elected")
	}

	const command = "set x=1"
	index, messages, err := nodes[leader].Propose(command)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Node %d proposed %q at index %d\n", leader, command, index)
	deliver(nodes, messages)

	for _, id := range members {
		entries := nodes[id].Committed(index - 1)
		if len(entries) != 1 || entries[0].NoOp || entries[0].Command != command {
			log.Fatalf("node %d has not committed the command", id)
		}
		fmt.Printf("Node %d committed %q at index %d\n", id, entries[0].Command, index)
	}
}

// Deliver messages and their replies in FIFO order over an in-memory network.
func deliver(nodes map[raft.ID]*raft.Node, messages []raft.Message) {
	for len(messages) > 0 {
		message := messages[0]
		messages = messages[1:]
		replies, err := nodes[message.To].Step(message)
		if err != nil {
			log.Fatal(err)
		}
		messages = append(messages, replies...)
	}
}
