package kv

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const raftTickInterval = 100 * time.Millisecond

type writeResult struct {
	err error
}

type RaftNode struct {
	id          uint64
	node        raft.Node
	raftStorage *raft.MemoryStorage
	store       *Store

	requestSeq atomic.Uint64

	pendingWriteMu sync.Mutex
	pendingWrites  map[string]chan writeResult

	pendingReadMu sync.Mutex
	pendingReads  map[string]chan uint64

	appliedMu      sync.Mutex
	appliedIndex   uint64
	appliedChanged chan struct{}
}

func NewRaftNode(id uint64, peers []raft.Peer, store *Store) *RaftNode {
	raftStorage := raft.NewMemoryStorage()
	cfg := &raft.Config{
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         raftStorage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}
	raftNode := raft.StartNode(cfg, peers)
	return &RaftNode{
		id:             id,
		node:           raftNode,
		raftStorage:    raftStorage,
		store:          store,
		pendingWrites:  make(map[string]chan writeResult),
		pendingReads:   make(map[string]chan uint64),
		appliedChanged: make(chan struct{}),
	}
}

func (n *RaftNode) applyCommittedEntry(entry *raftpb.Entry) (*Command, error) {
	switch entry.GetType() {
	case raftpb.EntryNormal:
		if len(entry.GetData()) == 0 {
			return nil, nil
		}

		cmd, err := DecodeCommand(entry.GetData())
		if err != nil {
			return nil, err
		}
		return &cmd, n.store.Apply(cmd)

	case raftpb.EntryConfChange:
		var change raftpb.ConfChange
		if err := proto.Unmarshal(entry.GetData(), &change); err != nil {
			return nil, err
		}
		n.node.ApplyConfChange(&change)

	case raftpb.EntryConfChangeV2:
		var change raftpb.ConfChangeV2
		if err := proto.Unmarshal(entry.GetData(), &change); err != nil {
			return nil, err
		}
		n.node.ApplyConfChange(&change)
	}

	return nil, nil
}

func (n *RaftNode) saveReadyToMemory(rd raft.Ready) error {
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := n.raftStorage.ApplySnapshot(rd.Snapshot); err != nil {
			return err
		}
	}

	if !raft.IsEmptyHardState(rd.HardState) {
		if err := n.raftStorage.SetHardState(rd.HardState); err != nil {
			return err
		}
	}

	if len(rd.Entries) > 0 {
		if err := n.raftStorage.Append(rd.Entries); err != nil {
			return err
		}
	}

	return nil
}

func (n *RaftNode) Propose(ctx context.Context, cmd Command) error {
	data, err := EncodeCommand(cmd)
	if err != nil {
		return err
	}
	return n.node.Propose(ctx, data)
}

// ProposeAndWait 等待 leader 应用已提交的命令，并收到对应结果。
func (n *RaftNode) ProposeAndWait(ctx context.Context, cmd Command) (string, error) {
	if cmd.RequestID == "" {
		cmd.RequestID = fmt.Sprintf("%d-%d", n.id, n.requestSeq.Add(1))
	}
	cmd.OriginNodeID = n.id

	resultCh := make(chan writeResult, 1)
	n.pendingWriteMu.Lock()
	if _, exists := n.pendingWrites[cmd.RequestID]; exists {
		n.pendingWriteMu.Unlock()
		return "", fmt.Errorf("request ID %q is already in flight", cmd.RequestID)
	}
	n.pendingWrites[cmd.RequestID] = resultCh
	n.pendingWriteMu.Unlock()

	if err := n.Propose(ctx, cmd); err != nil {
		n.removeWriteWaiter(cmd.RequestID)
		return cmd.RequestID, err
	}
	// 阻塞
	select {
	case result := <-resultCh:
		return cmd.RequestID, result.err
	case <-ctx.Done():
		n.removeWriteWaiter(cmd.RequestID)
		return cmd.RequestID, ctx.Err()
	}
}

func (n *RaftNode) removeWriteWaiter(requestID string) {
	n.pendingWriteMu.Lock()
	delete(n.pendingWrites, requestID)
	n.pendingWriteMu.Unlock()
}

func (n *RaftNode) CompleteWriteResult(result WriteResult) {
	if result.OriginNodeID != n.id {
		return
	}

	n.pendingWriteMu.Lock()
	resultCh, ok := n.pendingWrites[result.RequestID]
	if ok {
		delete(n.pendingWrites, result.RequestID)
	}
	n.pendingWriteMu.Unlock()
	if !ok {
		return
	}

	var err error
	if result.Error != "" {
		err = errors.New(result.Error)
	}
	resultCh <- writeResult{err: err}
}

func (n *RaftNode) LinearizableGet(ctx context.Context, key string) (string, bool, error) {
	requestID := fmt.Sprintf("read-%d-%d", n.id, n.requestSeq.Add(1))
	readStateCh := make(chan uint64, 1)

	n.pendingReadMu.Lock()
	n.pendingReads[requestID] = readStateCh
	n.pendingReadMu.Unlock()
	// 安全索引
	if err := n.node.ReadIndex(ctx, []byte(requestID)); err != nil {
		n.removeReadWaiter(requestID)
		return "", false, err
	}

	var readIndex uint64

	//阻塞等待
	select {
	case readIndex = <-readStateCh:
		n.removeReadWaiter(requestID)
	case <-ctx.Done():
		n.removeReadWaiter(requestID)
		return "", false, ctx.Err()
	}

	if err := n.waitApplied(ctx, readIndex); err != nil {
		return "", false, err
	}

	value, found := n.store.Get(key)
	return value, found, nil
}

func (n *RaftNode) removeReadWaiter(requestID string) {
	n.pendingReadMu.Lock()
	delete(n.pendingReads, requestID)
	n.pendingReadMu.Unlock()
}

func (n *RaftNode) handleReadStates(readStates []raft.ReadState) {
	for _, state := range readStates {
		requestID := string(state.RequestCtx)
		n.pendingReadMu.Lock()
		readStateCh, ok := n.pendingReads[requestID]
		if ok {
			delete(n.pendingReads, requestID)
		}
		n.pendingReadMu.Unlock()
		if ok {
			readStateCh <- state.Index
		}
	}
}

func (n *RaftNode) waitApplied(ctx context.Context, index uint64) error {
	for {
		n.appliedMu.Lock()
		if n.appliedIndex >= index {
			n.appliedMu.Unlock()
			return nil
		}
		changed := n.appliedChanged
		n.appliedMu.Unlock()

		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (n *RaftNode) markApplied(index uint64) {
	n.appliedMu.Lock()
	defer n.appliedMu.Unlock()
	if index <= n.appliedIndex {
		return
	}
	n.appliedIndex = index
	close(n.appliedChanged)
	n.appliedChanged = make(chan struct{})
}

func (n *RaftNode) Step(ctx context.Context, msg *raftpb.Message) error {
	return n.node.Step(ctx, msg)
}

func (n *RaftNode) Run(ctx context.Context, transport Transport) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer n.node.Stop()

	go transport.Run(runCtx)

	ticker := time.NewTicker(raftTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-runCtx.Done():
			return nil
		case <-ticker.C:
			n.node.Tick()
		case rd, ok := <-n.node.Ready():
			if !ok {
				return nil
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				return fmt.Errorf("apply snapshot to KV store: not implemented")
			}

			if err := n.saveReadyToMemory(rd); err != nil {
				return fmt.Errorf("save raft Ready to memory: %w", err)
			}

			for _, msg := range rd.Messages {
				if err := transport.Send(runCtx, msg); err != nil {
					if errors.Is(err, ErrTransportQueueFull) {
						log.Printf("drop raft message to node %d: transport queue full", msg.GetTo())
						continue
					}
					if runCtx.Err() != nil {
						return nil
					}
					return fmt.Errorf("enqueue raft message to node %d: %w", msg.GetTo(), err)
				}
			}

			n.handleReadStates(rd.ReadStates)
			for _, entry := range rd.CommittedEntries {
				cmd, err := n.applyCommittedEntry(entry)
				if cmd == nil && err != nil {
					return fmt.Errorf("apply committed raft entry %d: %w", entry.GetIndex(), err)
				}
				n.markApplied(entry.GetIndex())
				if cmd != nil && cmd.RequestID != "" && n.node.Status().RaftState == raft.StateLeader {
					result := WriteResult{OriginNodeID: cmd.OriginNodeID, RequestID: cmd.RequestID}
					if err != nil {
						result.Error = err.Error()
					}
					if result.OriginNodeID == n.id {
						n.CompleteWriteResult(result)
					} else if sendErr := transport.SendResult(runCtx, result); sendErr != nil {
						log.Printf("send write result %q to node %d: %v", result.RequestID, result.OriginNodeID, sendErr)
					}
				}
			}

			n.node.Advance()
		}
	}
}
