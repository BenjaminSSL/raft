# Raft in Go

This package implements leader election and replicated logs for a fixed group
of voting nodes. It uses only the Go standard library.

Start with [election.go](election.go) for voting and term changes, then read
[replication.go](replication.go) for log matching, conflict repair, and commits.
[raft.go](raft.go) defines the node API and persistence boundary.
[raft_test.go](raft_test.go) shows how to drive several nodes and simulate a
network partition.

## Run the demo

From the project directory, run:

```sh
go run ./cmd/raft-demo
```

The demo starts three in-memory nodes, prints the election result, and shows
each node committing the same command.

## Driving a node

1. Call `New` with a unique ID, the full member list, tick intervals, and a
   storage instance for that node. Use `MemoryStorage` for an in-process simulation.
2. Call `Tick` on each node at regular intervals. Route every returned message
   to its destination's `Step` method and route the replies the same way.
   Serialize calls to each node. Use the same tick duration across the cluster.
3. Call `Propose` on the leader. Its returned index identifies an appended
   entry. Wait for the command to appear in `Committed` before reporting success
   to a client. A later leader can replace an uncommitted entry at that index.
4. Call `Committed(lastApplied)` after ticks, messages, or proposals. Apply
   entries in order and advance the applied index for every entry, including
   no-ops. Each new leader appends a no-op so it can commit inherited entries
   even if no client submits a command.

The caller supplies the clock, message transport, and state machine. For
recovery across process restarts, supply a `Storage` implementation that saves
state atomically and durably, and retain your state machine's applied index
with its state. A save error stops the node. The in-memory store supports
simulated restarts when reused by a new node in the same process.

This implementation assumes trusted members. It sends the available log suffix
on each append, backs up one index per rejected probe, and saves the full state
when it changes. Logs grow without bound. Membership changes, snapshots, and
client request deduplication are outside its scope.

## Tests

```sh
go test ./...
go test -race ./...
```

[Raft paper](https://raft.github.io/raft.pdf)
