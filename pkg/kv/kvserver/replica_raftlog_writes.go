// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func (r *replicaLogStorage) stateRaftMuLocked() logstore.RaftState {
	return logstore.RaftState{
		LastIndex: r.shMu.lastIndexNotDurable,
		LastTerm:  r.shMu.lastTermNotDurable,
		ByteSize:  r.shMu.raftLogSize,
	}
}

func (r *replicaLogStorage) appendRaftMuLocked(
	ctx context.Context, app logstore.MsgStorageAppend, stats *logstore.AppendStats,
) (logstore.RaftState, error) {
	state := r.stateRaftMuLocked()
	cb := (*replicaSyncCallback)(r)
	return r.raftMu.logStorage.StoreEntries(ctx, state, app, cb, stats)
}

func (r *replicaLogStorage) updateStateRaftMuLockedMuLocked(state logstore.RaftState) {
	r.shMu.lastIndexNotDurable = state.LastIndex
	r.shMu.lastTermNotDurable = state.LastTerm
	r.shMu.raftLogSize = state.ByteSize
}

// replicaSyncCallback implements the logstore.SyncCallback interface.
type replicaSyncCallback Replica

func (r *replicaSyncCallback) OnLogSync(
	ctx context.Context, done logstore.MsgStorageAppendDone, commitStats storage.BatchCommitStats,
) {
	repl := (*Replica)(r)
	// The log mark is non-empty only if this was a non-empty log append that
	// updated the stable log mark.
	if mark := done.Mark(); mark.Term != 0 {
		repl.flowControlV2.SyncedLogStorage(ctx, mark)
	}
	// Block sending the responses back to raft, if a test needs to.
	if fn := repl.store.TestingKnobs().TestingAfterRaftLogSync; fn != nil {
		fn(repl.ID())
	}
	// Send MsgStorageAppend's responses.
	repl.sendRaftMessages(ctx, done.Responses(), nil /* blocked */, false /* willDeliverLocal */)
	if commitStats.TotalDuration > defaultReplicaRaftMuWarnThreshold {
		log.Infof(repl.raftCtx, "slow non-blocking raft commit: %s", commitStats)
	}
}

func (r *replicaSyncCallback) OnSnapSync(ctx context.Context, done logstore.MsgStorageAppendDone) {
	repl := (*Replica)(r)
	// NB: when storing snapshot, done always contains a non-zero log mark.
	repl.flowControlV2.SyncedLogStorage(ctx, done.Mark())
	repl.sendRaftMessages(ctx, done.Responses(), nil /* blocked */, true /* willDeliverLocal */)
}
