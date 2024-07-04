// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package raft

import pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"

// logWrite is blah.
//
// The rationale for having a logWrite without the HardState.Term is that it can
// be used by a Learner who has no HardState.
type logWrite struct {
	snap pb.Snapshot
	ls   logSlice
}

type logWrites struct {
	u        *unstable
	sentSnap uint64

	// Invariant: acked <= sent.
	sent  logMark
	acked logMark
}

func (w *logWrites) hasNext() bool {
	return w.u.snapshot != nil && w.sentSnap != w.u.snapshot.Metadata.Index ||
		len(w.u.entries) != 0 && w.sent != w.u.mark()
}

func (w *logWrites) next() logWrite {
	var lw logWrite
	if snap := w.u.snapshot; snap != nil && w.sentSnap != snap.Metadata.Index {
		lw.snap = *snap
	}
	if len(w.u.entries) != 0 && w.sent != w.u.mark() {
		lw.ls = w.u.logSlice.forward(w.sent.index)
	}
	return lw
}

// TODO: remove accept
func (w *logWrites) accept() {
	if snap := w.u.snapshot; snap != nil {
		w.sentSnap = snap.Metadata.Index
	}
	if len(w.u.entries) != 0 && w.sent != w.u.mark() {
		// TODO(pav-kv): assert the w.sent is monotonic.
		w.sent = w.u.mark()
	}
	w.u.acceptInProgress()
}

func (w *logWrites) ackSnap(index uint64) {
	if w.u.snapshot == nil {
		panic("")
	}
	if index == w.u.snapshot.Metadata.Index {
		w.u.snapshot = nil
	}
}

func (w *logWrites) ack(ack logMark) {
	if !w.acked.less(ack) {
		return
	}
	if ack.term != w.sent.term {
		return
	}
	if snap := w.u.snapshot; snap != nil {
		if ack.index > snap.Metadata.Index {
			panic("")
		}
		w.acked = ack
		return
	}
	w.acked = ack
	w.u.logSlice = w.u.logSlice.forward(ack.index)
}
