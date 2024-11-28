// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
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

func (r *replicaLogStorage) compactRaftMuLocked(
	ctx context.Context,
	prev kvserverpb.RaftTruncatedState,
	next *kvserverpb.RaftTruncatedState,
	batch storage.ReadWriter,
) (apply bool, _ error) {
	r.raftMu.AssertHeld()
	return logstore.Compact(ctx, prev, next, r.raftMu.stateLoader.StateLoader, batch)
}

func (r *replicaLogStorage) compactPostApplyRaftMuLocked(
	ctx context.Context, t kvserverpb.RaftTruncatedState,
) int64 {
	r.raftMu.AssertHeld()
	r.mu.Lock()
	r.shMu.raftTruncState = t
	r.mu.Unlock()

	// Clear any entries in the Raft log entry cache for this range up to and
	// including the most recently truncated index.
	r.store.raftEntryCache.Clear(r.RangeID, t.Index+1)

	// Truncate the sideloaded storage. This is safe only if the new truncated
	// state is durably stored on disk, i.e. synced.
	// TODO(#38566, #113135): this is unfortunately not true, need to fix this.
	//
	// TODO(sumeer): once we remove the legacy caller of
	// handleTruncatedStateResult, stop calculating the size of the removed files
	// and the remaining files.
	log.Eventf(ctx, "truncating sideloaded storage up to (and including) index %d", t.Index)
	size, _, err := r.raftMu.logStorage.Sideload.TruncateTo(ctx, t.Index+1)
	if err != nil {
		// We don't *have* to remove these entries for correctness. Log a
		// loud error, but keep humming along.
		log.Errorf(ctx, "while removing sideloaded files during log truncation: %+v", err)
	}
	// NB: we don't sync the sideloaded entry files removal here for performance
	// reasons. If a crash occurs, and these files get recovered after a restart,
	// we should clean them up on the server startup.
	//
	// TODO(#113135): this removal survives process crashes though, and system
	// crashes if the filesystem is quick enough to sync it for us. Add a test
	// that syncs the files removal here, and "crashes" right after, to help
	// reproduce and fix #113135.
	return size
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
