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
	"sync/atomic"
	"unsafe"
)

type Node[T any] struct {
	Next  *Node[T]
	Value T
}

// QueueRLocked is a subset of Queue available when the enclosing read lock is
// locked.
type QueueRLocked[T any] Queue[T]

// QueueLocked is a subset of Queue available when the enclosing write lock is
// locked.
type QueueLocked[T any] QueueRLocked[T]

// Queue is an intrusive multi-producer single-consumer queue.
type Queue[T any] struct {
	head Node[T]
	tail unsafe.Pointer // *Node[T]
	len  uint64
}

// NewQueue returns a new empty queue.
func NewQueue[T any]() *Queue[T] {
	q := &Queue[T]{}
	q.tail = unsafe.Pointer(&q.head)
	return q
}

func (q *Queue[T]) RLocked() *QueueRLocked[T] { return (*QueueRLocked[T])(q) }
func (q *Queue[T]) Locked() *QueueLocked[T]   { return (*QueueLocked[T])(q) }

func (q *QueueRLocked[T]) Len() uint64 {
	return atomic.LoadUint64(&q.len)
}

func (q *QueueLocked[T]) Len() uint64 {
	return q.len
}

// Push pushes the given node to the queue. Returns true if this is the first
// entry in the queue, in which case the caller may signal the reader.
//
// The enclosing lock must be locked for reads.
func (q *QueueRLocked[T]) Push(n *Node[T]) bool {
	atomic.AddUint64(&q.len, 1)
	n.Next = nil
	was := (*Node[T])(atomic.SwapPointer(&q.tail, unsafe.Pointer(n)))
	was.Next = n
	return was == &q.head
}

// Push pushes the given node to the queue. Returns true if this is the first
// entry in the queue, in which case the caller may signal the reader.
//
// The enclosing lock must be locked for writes.
func (q *QueueLocked[T]) Push(n *Node[T]) bool {
	q.len++
	n.Next = nil
	(*Node[T])(q.tail).Next = n
	q.tail = unsafe.Pointer(n)
	return q.head.Next == n
}

// Pop pops the first entry from the queue and returns it. Returns nil if the
// queue is empty.
//
// The enclosing lock must be locked for writes.
func (q *QueueLocked[T]) Pop() *Node[T] {
	first := q.head.Next
	if first == nil {
		return nil
	}
	q.head.Next, first.Next = first.Next, nil
	if q.head.Next == nil {
		q.tail = unsafe.Pointer(&q.head)
	}
	q.len--
	return first
}

// Flush clears the queue and returns it as a linked list, connected by
// Node[T].Next pointers. Returns nil if the queue is empty.
//
// The enclosing lock must be locked for writes.
func (q *QueueLocked[T]) Flush() List[T] {
	first := q.head.Next
	if first == nil {
		return List[T]{}
	}
	q.head.Next = nil
	q.tail = unsafe.Pointer(&q.head)
	ln := q.len
	q.len = 0
	return List[T]{first: first, len: ln}
}

type List[T any] struct {
	first *Node[T]
	len   uint64
}

func (l List[T]) First() *Node[T] { return l.first }

func (l List[T]) Split(at *Node[T], ln uint64) (List[T], List[T]) {
	next := at.Next
	at.Next = nil
	return List[T]{first: l.first, len: ln},
		List[T]{first: next, len: l.len - ln}
}

func (l List[T]) Len() uint64 { return l.len }

func (l List[T]) Unlink() []T {
	values := make([]T, 0, l.Len())
	for n := l.first; n != nil; {
		values = append(values, n.Value)
		n, n.Next = n.Next, nil
	}
	return values
}
