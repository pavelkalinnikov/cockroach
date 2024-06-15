// This code has been modified from its original form by Cockroach Labs, Inc.
// All modifications are Copyright 2024 Cockroach Labs, Inc.
//
// Copyright 2022 The etcd Authors
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

package rafttest

import (
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/datadriven"
)

func (env *InteractionEnv) handleProcessAppendThread(t *testing.T, d datadriven.TestData) error {
	idxs := nodeIdxs(t, d)
	for _, idx := range idxs {
		var err error
		if len(idxs) > 1 {
			fmt.Fprintf(env.Output, "> %d processing append thread\n", idx+1)
			env.withIndent(func() { err = env.ProcessAppendThread(idx) })
		} else {
			err = env.ProcessAppendThread(idx)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// ProcessAppendThread runs processes a single message on the "append" thread of
// the node with the given index.
func (env *InteractionEnv) ProcessAppendThread(idx int) error {
	n := &env.Nodes[idx]
	if len(n.AppendWork) == 0 {
		env.Output.WriteString("no append work to perform")
		return nil
	}
	m := n.AppendWork[0]
	n.AppendWork = n.AppendWork[1:]

	resps := m.Responses
	m.Responses = nil
	env.Output.WriteString("Processing:\n")
	env.Output.WriteString(
		raft.DescribeMessage(msgFromStorageReady(m, uint64(idx+1)), defaultEntryFormatter) + "\n")

	if err := processAppend(n, m); err != nil {
		return err
	}

	env.Output.WriteString("Responses:\n")
	for _, m := range resps {
		env.Output.WriteString(raft.DescribeMessage(m, defaultEntryFormatter) + "\n")
	}
	env.Messages = append(env.Messages, resps...)
	return nil
}

func processAppend(n *Node, rd raft.StorageReady) error {
	// TODO(tbg): the order of operations here is not necessarily safe. See:
	// https://github.com/etcd-io/etcd/pull/10861
	s := n.Storage
	if st := rd.HardState; !raft.IsEmptyHardState(st) {
		if err := s.SetHardState(st); err != nil {
			return err
		}
	}
	if snap := rd.Snapshot; !raft.IsEmptySnap(snap) {
		if len(rd.Entries) > 0 {
			return errors.New("can't apply snapshot and entries at the same time")
		}
		return s.ApplySnapshot(snap)
	}
	return s.Append(rd.Entries)
}
