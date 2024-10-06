// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 2024 The Cockroach Authors.
//
// Copyright 2024 The etcd Authors
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

func TestEntryID(t *testing.T) {
	// Some obvious checks first.
	require.Equal(t, EntryID{Term: 5, Index: 10}, EntryID{Term: 5, Index: 10})
	require.NotEqual(t, EntryID{Term: 4, Index: 10}, EntryID{Term: 5, Index: 10})
	require.NotEqual(t, EntryID{Term: 5, Index: 9}, EntryID{Term: 5, Index: 10})

	for _, tt := range []struct {
		entry pb.Entry
		want  EntryID
	}{
		{entry: pb.Entry{}, want: EntryID{Term: 0, Index: 0}},
		{entry: pb.Entry{Term: 1, Index: 2, Data: []byte("data")}, want: EntryID{Term: 1, Index: 2}},
		{entry: pb.Entry{Term: 10, Index: 123}, want: EntryID{Term: 10, Index: 123}},
	} {
		require.Equal(t, tt.want, pbEntryID(&tt.entry))
	}
}

func TestLogSlice(t *testing.T) {
	id := func(index, term uint64) EntryID {
		return EntryID{Term: term, Index: index}
	}
	e := func(index, term uint64) pb.Entry {
		return pb.Entry{Term: term, Index: index}
	}
	for _, tt := range []struct {
		term    uint64
		prev    EntryID
		entries []pb.Entry

		notOk bool
		last  EntryID
	}{
		// Empty "dummy" slice, starting at (0, 0) origin of the log.
		{last: id(0, 0)},
		// Empty slice with a given prev ID. Valid only if term >= prev.term.
		{prev: id(123, 10), notOk: true},
		{term: 9, prev: id(123, 10), notOk: true},
		{term: 10, prev: id(123, 10), last: id(123, 10)},
		{term: 11, prev: id(123, 10), last: id(123, 10)},
		// A single entry.
		{term: 0, entries: []pb.Entry{e(1, 1)}, notOk: true},
		{term: 1, entries: []pb.Entry{e(1, 1)}, last: id(1, 1)},
		{term: 2, entries: []pb.Entry{e(1, 1)}, last: id(1, 1)},
		// Multiple entries.
		{term: 2, entries: []pb.Entry{e(2, 1), e(3, 1), e(4, 2)}, notOk: true},
		{term: 1, prev: id(1, 1), entries: []pb.Entry{e(2, 1), e(3, 1), e(4, 2)}, notOk: true},
		{term: 2, prev: id(1, 1), entries: []pb.Entry{e(2, 1), e(3, 1), e(4, 2)}, last: id(4, 2)},
		// First entry inconsistent with prev.
		{term: 10, prev: id(123, 5), entries: []pb.Entry{e(111, 5)}, notOk: true},
		{term: 10, prev: id(123, 5), entries: []pb.Entry{e(124, 4)}, notOk: true},
		{term: 10, prev: id(123, 5), entries: []pb.Entry{e(234, 6)}, notOk: true},
		{term: 10, prev: id(123, 5), entries: []pb.Entry{e(124, 6)}, last: id(124, 6)},
		// Inconsistent entries.
		{term: 10, prev: id(12, 2), entries: []pb.Entry{e(13, 2), e(12, 2)}, notOk: true},
		{term: 10, prev: id(12, 2), entries: []pb.Entry{e(13, 2), e(15, 2)}, notOk: true},
		{term: 10, prev: id(12, 2), entries: []pb.Entry{e(13, 2), e(14, 1)}, notOk: true},
		{term: 10, prev: id(12, 2), entries: []pb.Entry{e(13, 2), e(14, 3)}, last: id(14, 3)},
	} {
		t.Run("", func(t *testing.T) {
			s := LogSlice{term: tt.term, prev: tt.prev, entries: tt.entries}
			require.Equal(t, tt.notOk, s.valid() != nil)
			if tt.notOk {
				return
			}
			last := s.lastEntryID()
			require.Equal(t, tt.last, last)
			require.Equal(t, last.Index, s.lastIndex())
			require.Equal(t, LogMark{Term: tt.term, Index: last.Index}, s.mark())

			require.Equal(t, tt.prev.Term, s.termAt(tt.prev.Index))
			for _, e := range tt.entries {
				require.Equal(t, e.Term, s.termAt(e.Index))
			}
		})
	}
}

func TestLogSliceForward(t *testing.T) {
	id := func(index, term uint64) EntryID {
		return EntryID{Term: term, Index: index}
	}
	ls := func(prev EntryID, terms ...uint64) LogSlice {
		empty := make([]pb.Entry, 0) // hack to canonicalize empty slices
		return LogSlice{
			term:    8,
			prev:    prev,
			entries: append(empty, index(prev.Index+1).terms(terms...)...),
		}
	}
	for _, tt := range []struct {
		ls   LogSlice
		to   uint64
		want LogSlice
	}{
		{ls: LogSlice{}, to: 0, want: LogSlice{}},
		{ls: ls(id(5, 1)), to: 5, want: ls(id(5, 1))},
		{ls: ls(id(10, 3), 3, 4, 5), to: 10, want: ls(id(10, 3), 3, 4, 5)},
		{ls: ls(id(10, 3), 3, 4, 5), to: 11, want: ls(id(11, 3), 4, 5)},
		{ls: ls(id(10, 3), 3, 4, 5), to: 12, want: ls(id(12, 4), 5)},
		{ls: ls(id(10, 3), 3, 4, 5), to: 13, want: ls(id(13, 5))},
	} {
		t.Run("", func(t *testing.T) {
			require.NoError(t, tt.ls.valid())
			require.Equal(t, tt.want, tt.ls.forward(tt.to))
		})
	}
}

func TestSnapshot(t *testing.T) {
	id := func(index, term uint64) EntryID {
		return EntryID{Term: term, Index: index}
	}
	snap := func(index, term uint64) pb.Snapshot {
		return pb.Snapshot{Metadata: pb.SnapshotMetadata{
			Term: term, Index: index,
		}}
	}
	for _, tt := range []struct {
		term  uint64
		snap  pb.Snapshot
		notOk bool
		last  EntryID
	}{
		// Empty "dummy" snapshot, at (0, 0) origin of the log.
		{last: id(0, 0)},
		// Valid only if term >= Metadata.Term.
		{term: 10, snap: snap(123, 9), last: id(123, 9)},
		{term: 10, snap: snap(123, 10), last: id(123, 10)},
		{term: 10, snap: snap(123, 11), notOk: true},
	} {
		t.Run("", func(t *testing.T) {
			s := snapshot{term: tt.term, snap: tt.snap}
			require.Equal(t, tt.notOk, s.valid() != nil)
			if tt.notOk {
				return
			}
			last := s.lastEntryID()
			require.Equal(t, tt.last, last)
			require.Equal(t, last.Index, s.lastIndex())
			require.Equal(t, LogMark{Term: tt.term, Index: last.Index}, s.mark())
		})
	}
}
