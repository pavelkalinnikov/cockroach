// This code has been modified from its original form by Cockroach Labs, Inc.
// All modifications are Copyright 2024 Cockroach Labs, Inc.
//
// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"testing"

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/stretchr/testify/require"
)

func newUnstableForTesting(snap *pb.Snapshot, ls logSlice) unstable {
	return unstable{
		snapshot:   snap,
		logSlice:   ls,
		inProgress: ls.prev.index,
		logger:     discardLogger,
	}
}

func (u *unstable) checkInvariants(t testing.TB) {
	t.Helper()
	require.NoError(t, u.logSlice.valid())
	require.LessOrEqual(t, u.inProgress, u.lastIndex())
	if u.snapshot != nil {
		require.Equal(t, u.snapshot.Metadata.Term, u.prev.term)
		require.Equal(t, u.snapshot.Metadata.Index, u.prev.index)
		require.True(t, u.inProgress == 0 || u.inProgress >= u.prev.index)
	} else {
		require.GreaterOrEqual(t, u.inProgress, u.prev.index)
	}
}

func TestUnstableMaybeFirstIndex(t *testing.T) {
	prev4 := entryID{term: 1, index: 4}
	snap4 := &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}}
	for _, tt := range []struct {
		snap *pb.Snapshot
		ls   logSlice

		wok    bool
		windex uint64
	}{
		// no snapshot
		{
			nil, prev4.append(1),
			false, 0,
		},
		{
			nil, logSlice{},
			false, 0,
		},
		// has snapshot
		{
			snap4, prev4.append(1),
			true, 5,
		},
		{
			snap4, prev4.append(),
			true, 5,
		},
	} {
		t.Run("", func(t *testing.T) {
			u := newUnstableForTesting(tt.snap, tt.ls)
			u.checkInvariants(t)

			index, ok := u.maybeFirstIndex()
			require.Equal(t, tt.wok, ok)
			require.Equal(t, tt.windex, index)
		})
	}
}

func TestLastIndex(t *testing.T) {
	prev4 := entryID{term: 1, index: 4}
	snap4 := &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}}
	for _, tt := range []struct {
		snap   *pb.Snapshot
		ls     logSlice
		windex uint64
	}{
		{nil, prev4.append(1), 5}, // last in entries
		{snap4, prev4.append(1), 5},
		{snap4, prev4.append(), 4}, // last in snapshot
		{nil, logSlice{}, 0},       // empty unstable
	} {
		t.Run("", func(t *testing.T) {
			u := newUnstableForTesting(tt.snap, tt.ls)
			u.checkInvariants(t)
			require.Equal(t, tt.windex, u.lastIndex())
		})
	}
}

func TestUnstableMaybeTerm(t *testing.T) {
	prev4 := entryID{term: 1, index: 4}
	snap4 := &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}}
	for _, tt := range []struct {
		snap  *pb.Snapshot
		ls    logSlice
		index uint64

		wok   bool
		wterm uint64
	}{
		// term from entries
		{
			nil, prev4.append(1),
			5,
			true, 1,
		},
		{
			nil, prev4.append(1),
			6,
			false, 0,
		},
		{
			nil, prev4.append(1),
			4,
			true, 1,
		},
		{
			snap4, prev4.append(1),
			5,
			true, 1,
		},
		{
			snap4, prev4.append(1),
			6,
			false, 0,
		},
		// term from snapshot
		{
			snap4, prev4.append(1),
			4,
			true, 1,
		},
		{
			snap4, prev4.append(1),
			3,
			false, 0,
		},
		{
			snap4, prev4.append(),
			5,
			false, 0,
		},
		{
			snap4, prev4.append(),
			4,
			true, 1,
		},
		{
			nil, prev4.append(),
			5,
			false, 0,
		},
	} {
		t.Run("", func(t *testing.T) {
			u := newUnstableForTesting(tt.snap, tt.ls)
			u.checkInvariants(t)

			term, ok := u.maybeTerm(tt.index)
			require.Equal(t, tt.wok, ok)
			require.Equal(t, tt.wterm, term)
		})
	}
}

func TestUnstableRestore(t *testing.T) {
	u := newUnstableForTesting(
		&pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
		entryID{term: 1, index: 4}.append(1),
	)
	u.inProgress = 5
	u.checkInvariants(t)

	s := snapshot{
		term: 2,
		snap: pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 6, Term: 2}},
	}
	require.True(t, u.restore(s))
	u.checkInvariants(t)

	require.Zero(t, u.inProgress)
	require.Zero(t, len(u.entries))
	require.Equal(t, &s.snap, u.snapshot)
}

func TestUnstableNextEntries(t *testing.T) {
	prev4 := entryID{term: 1, index: 4}
	for _, tt := range []struct {
		ls         logSlice
		inProgress uint64

		wentries []pb.Entry
	}{
		// nothing in progress
		{
			prev4.append(1, 1), 4,
			index(5).terms(1, 1),
		},
		// partially in progress
		{
			prev4.append(1, 1), 5,
			index(6).terms(1),
		},
		// everything in progress
		{
			prev4.append(1, 1), 6,
			nil, // nil, not empty slice
		},
	} {
		t.Run("", func(t *testing.T) {
			u := newUnstableForTesting(nil /* snap */, tt.ls)
			u.inProgress = tt.inProgress
			u.checkInvariants(t)
			require.Equal(t, tt.wentries, u.nextEntries())
		})
	}
}

func TestUnstableNextSnapshot(t *testing.T) {
	prev4 := entryID{term: 1, index: 4}
	snap4 := &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}}
	for _, tt := range []struct {
		snap       *pb.Snapshot
		prev       entryID
		inProgress uint64

		wsnapshot *pb.Snapshot
	}{
		// snapshot not unstable
		{
			nil, entryID{}, 0,
			nil,
		},
		// snapshot not in progress
		{
			snap4, prev4, 0,
			snap4,
		},
		// snapshot in progress
		{
			snap4, prev4, 4,
			nil,
		},
	} {
		t.Run("", func(t *testing.T) {
			u := newUnstableForTesting(tt.snap, tt.prev.append())
			u.inProgress = tt.inProgress
			u.checkInvariants(t)
			require.Equal(t, tt.wsnapshot, u.nextSnapshot())
		})
	}
}

func TestUnstableAcceptInProgress(t *testing.T) {
	prev4 := entryID{term: 1, index: 4}
	snap4 := &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}}
	for _, tt := range []struct {
		snap        *pb.Snapshot
		ls          logSlice
		inProgress  uint64
		winProgress uint64
	}{
		{
			nil, prev4.append(), // no entries
			4, 4,
		},
		{
			nil, prev4.append(1),
			4, 5, // entries not in progress
		},
		{
			nil, prev4.append(1, 1),
			4, 6, // entries not in progress
		},
		{
			nil, prev4.append(1, 1),
			5, 6, // in-progress to the first entry
		},
		{
			nil, prev4.append(1, 1),
			6, 6, // in-progress to the second entry
		},
		// with snapshot
		{
			snap4, prev4.append(), // no entries
			0, 4, // snapshot not already in progress
		},
		{
			snap4, prev4.append(1),
			0, 5, // snapshot/entries not in progress
		},
		{
			snap4, prev4.append(1, 1),
			0, 6, // snapshot/entries not in progress
		},
		{
			snap4, prev4.append(),
			4, 4, // snapshot in progress, entries not in progress
		},
		{
			snap4, prev4.append(1),
			4, 5, // snapshot in progress, entries not in progress
		},
		{
			snap4, prev4.append(1, 1),
			4, 6, // snapshot in progress, entries not in progress
		},
		{
			snap4, prev4.append(1, 1),
			5, 6, // snapshot and first entry in progress
		},
		{
			snap4, prev4.append(1, 1),
			6, 6, // snapshot and two entries in progress
		},
	} {
		t.Run("", func(t *testing.T) {
			u := newUnstableForTesting(tt.snap, tt.ls)
			u.inProgress = tt.inProgress
			u.checkInvariants(t)

			u.acceptInProgress()
			u.checkInvariants(t)
			require.Equal(t, tt.winProgress, u.inProgress)
		})
	}
}

func TestUnstableStableTo(t *testing.T) {
	prev4 := entryID{term: 1, index: 4}
	snap4 := &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}}
	for _, tt := range []struct {
		snap        *pb.Snapshot
		ls          logSlice
		inProgress  uint64
		index, term uint64

		wprev       uint64
		winProgress uint64
		wlen        int
	}{
		{
			nil, logSlice{}, 0,
			5, 1,
			0, 0, 0,
		},
		{
			nil, prev4.append(1), 5,
			5, 1, // stable to the first entry
			5, 5, 0,
		},
		{
			nil, prev4.append(1, 1), 5,
			5, 1, // stable to the first entry
			5, 5, 1,
		},
		{
			nil, prev4.append(1, 1), 6,
			5, 1, // stable to the first entry and in-progress ahead
			5, 6, 1,
		},
		{
			nil, entryID{term: 1, index: 5}.append(2), 6,
			6, 1, // stable to the first entry and term mismatch
			5, 6, 1,
		},
		{
			nil, prev4.append(1), 5,
			4, 1, // stable to old entry
			4, 5, 1,
		},
		{
			nil, prev4.append(1), 5,
			4, 2, // stable to old entry
			4, 5, 1,
		},
		// with snapshot
		{
			snap4, prev4.append(1), 5,
			5, 1, // stable to the first entry
			5, 5, 0,
		},
		{
			snap4, prev4.append(1, 1), 5,
			5, 1, // stable to the first entry
			5, 5, 1,
		},
		{
			snap4, prev4.append(1, 1), 6,
			5, 1, // stable to the first entry and in-progress ahead
			5, 6, 1,
		},
		{
			&pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 5, Term: 1}},
			entryID{term: 1, index: 5}.append(2), 6,
			6, 1, // stable to the first entry and term mismatch
			5, 6, 1,
		},
		{
			snap4, prev4.append(1), 5,
			4, 1, // stable to snapshot
			4, 5, 1,
		},
		{
			snap4, prev4.append(2), 5,
			4, 1, // stable to old entry
			4, 5, 1,
		},
	} {
		t.Run("", func(t *testing.T) {
			u := newUnstableForTesting(tt.snap, tt.ls)
			u.inProgress = tt.inProgress
			u.checkInvariants(t)

			if u.snapshot != nil {
				u.stableSnapTo(u.snapshot.Metadata.Index)
			}
			u.checkInvariants(t)
			u.stableTo(entryID{term: tt.term, index: tt.index})
			u.checkInvariants(t)
			require.Equal(t, tt.wprev, u.prev.index)
			require.Equal(t, tt.winProgress, u.inProgress)
			require.Equal(t, tt.wlen, len(u.entries))
		})
	}
}

func TestUnstableTruncateAndAppend(t *testing.T) {
	prev4 := entryID{term: 1, index: 4}
	for _, tt := range []struct {
		snap       *pb.Snapshot
		ls         logSlice
		inProgress uint64
		app        logSlice

		want        logSlice
		winProgress uint64
	}{
		// append to the end
		{
			nil, prev4.append(1), 4,
			entryID{term: 1, index: 5}.append(1, 1),
			prev4.append(1, 1, 1),
			4,
		},
		{
			nil, prev4.append(1), 5,
			entryID{term: 1, index: 5}.append(1, 1),
			prev4.append(1, 1, 1),
			5,
		},
		// replace the unstable entries
		{
			nil, prev4.append(1), 4,
			prev4.append(2, 2),
			prev4.append(2, 2),
			4,
		},
		{
			nil, prev4.append(1), 4,
			entryID{term: 1, index: 3}.append(2, 2, 2),
			entryID{term: 1, index: 3}.append(2, 2, 2),
			3,
		},
		{
			nil, prev4.append(1), 5,
			prev4.append(2, 2),
			prev4.append(2, 2),
			4,
		},
		// truncate the existing entries and append
		{
			nil, prev4.append(1, 1, 1), 4,
			entryID{term: 1, index: 5}.append(2),
			prev4.append(1, 2),
			4,
		},
		{
			nil, prev4.append(1, 1, 1), 4,
			entryID{term: 1, index: 6}.append(2, 2),
			prev4.append(1, 1, 2, 2),
			4,
		},
		{
			nil, prev4.append(1, 1, 1), 5,
			entryID{term: 1, index: 5}.append(2),
			prev4.append(1, 2),
			5,
		},
		{
			nil, prev4.append(1, 1, 1), 6,
			entryID{term: 1, index: 5}.append(2),
			prev4.append(1, 2),
			5,
		},
	} {
		t.Run("", func(t *testing.T) {
			u := newUnstableForTesting(tt.snap, tt.ls)
			u.inProgress = tt.inProgress
			u.checkInvariants(t)

			u.truncateAndAppend(tt.app)
			u.checkInvariants(t)
			require.Equal(t, tt.want, u.logSlice)
			require.Equal(t, tt.winProgress, u.inProgress)
		})
	}
}
