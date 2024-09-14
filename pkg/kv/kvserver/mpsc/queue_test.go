// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mpsc

import (
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

func TestQueue(t *testing.T) {
	dirty := make(chan struct{}, 1)

	var mu syncutil.RWMutex
	q := NewQueue[int]()
	insert := func(x int) {
		mu.RLock()
		defer mu.RUnlock()
		t.Logf("insert: %d", x)
		if q.RLocked().Push(&Node[int]{Value: x}) {
			dirty <- struct{}{}
		}
	}

	worker := func(from, to int) {
		for i := from; i < to; i++ {
			insert(i)
		}
	}
	flush := func() {
		mu.Lock()
		defer mu.Unlock()
		t.Log("flushing")
		list := q.Locked().Flush()
		for n := list.first; n != nil; n = n.Next {
			t.Logf("popped: %d", n.Value)
		}
	}

	readerDone := make(chan struct{})
	reader := func() {
		defer close(readerDone)
		for {
			if _, ok := <-dirty; !ok {
				break
			}
			flush()
		}
	}
	go reader()

	const workers = 16
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			worker(i*5000, (i+1)*5000)
		}(i)
	}

	wg.Wait()
	close(dirty)
	<-readerDone
}

func BenchmarkQueue(b *testing.B) {
	b.ReportAllocs()
	test := func() {
		q := NewQueue[int]()
		for i := 0; i < 1000; i++ {
			q.RLocked().Push(&Node[int]{Value: 10})
		}
		l := q.Locked().Flush()
		_ = l
	}
	for n := b.N; n > 0; n-- {
		test()
	}
}

type proposalData struct {
	self *Node[*proposalData]
	data int
}

func newData(x int) *proposalData {
	s := struct {
		node Node[*proposalData]
		data proposalData
	}{}
	s.data = proposalData{data: x}
	s.node.Value = &s.data
	s.data.self = &s.node
	return &s.data
}

func BenchmarkIntrusiveNode(b *testing.B) {
	b.ReportAllocs()
	items := make([]*proposalData, 0, b.N)
	for n := b.N; n > 0; n-- {
		items = append(items, newData(10))
	}
	_ = items
}
