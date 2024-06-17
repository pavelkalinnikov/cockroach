// This code has been modified from its original form by Cockroach Labs, Inc.
// All modifications are Copyright 2024 Cockroach Labs, Inc.
//
// Copyright 2019 The etcd Authors
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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/datadriven"
)

func (env *InteractionEnv) handleProcessReady(t *testing.T, d datadriven.TestData) error {
	idxs := nodeIdxs(t, d)
	for _, idx := range idxs {
		var err error
		if len(idxs) > 1 {
			fmt.Fprintf(env.Output, "> %d handling Ready\n", idx+1)
			env.withIndent(func() { err = env.ProcessReady(idx) })
		} else {
			err = env.ProcessReady(idx)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func msgFromStorageReady(rd raft.StorageReady, nodeID uint64) raftpb.Message {
	return raftpb.Message{
		Type:      raftpb.MsgStorageAppend,
		From:      nodeID,
		To:        raft.LocalAppendThread,
		Term:      rd.HardState.Term,
		Vote:      rd.HardState.Vote,
		Commit:    rd.HardState.Commit,
		Entries:   rd.Entries,
		Responses: rd.Responses,
	}
}

func msgFromApplyReady(rd raft.ApplyReady, nodeID uint64) raftpb.Message {
	return raftpb.Message{
		Type:      raftpb.MsgStorageApply,
		From:      nodeID,
		To:        raft.LocalApplyThread,
		Entries:   rd.CommittedEntries,
		Responses: rd.Responses,
	}
}

// ProcessReady runs Ready handling on the node with the given index.
func (env *InteractionEnv) ProcessReady(idx int) error {
	// TODO(tbg): Allow simulating crashes here.
	n := &env.Nodes[idx]
	rd := n.Ready()

	// TODO(pav-kv): the code below is ugly because it tries to keep the output
	// format in tests unchanged. Clean it up in a separate commit.
	toLog := rd
	if n.Config.AsyncStorageWrites {
		if st := rd.StorageReady; !st.Empty() {
			toLog.Messages = append(toLog.Messages, msgFromStorageReady(st, uint64(idx+1)))
		}
		if app := rd.ApplyReady; len(app.CommittedEntries) != 0 {
			toLog.Messages = append(toLog.Messages, msgFromApplyReady(app, uint64(idx+1)))
		}
	} else {
		for _, m := range rd.StorageReady.Responses {
			if m.To != uint64(idx+1) {
				toLog.Messages = append(toLog.Messages, m)
			}
		}
	}
	env.Output.WriteString(raft.DescribeReady(toLog, defaultEntryFormatter))

	env.Messages = append(env.Messages, rd.Messages...)
	if !n.Config.AsyncStorageWrites {
		if err := processAppend(n, rd.StorageReady); err != nil {
			return err
		}
		if err := processApply(n, rd.CommittedEntries); err != nil {
			return err
		}
		// TODO(pav-kv): the code below is for compatibility in the test output. We
		// should just append(env.Messages, Responses...).
		for _, m := range rd.StorageReady.Responses {
			if m.To != uint64(idx+1) {
				env.Messages = append(env.Messages, m)
			}
		}
		n.Advance(rd)
	} else {
		if !rd.StorageReady.Empty() {
			n.AppendWork = append(n.AppendWork, rd.StorageReady)
		}
		if len(rd.ApplyReady.CommittedEntries) != 0 {
			n.ApplyWork = append(n.ApplyWork, rd.ApplyReady)
		}
	}

	return nil
}
