package mpsc

import "fmt"

type Node[T any] struct {
	Next  *Node[T]
	Value T
}

type List[T any] struct {
	first *Node[T]
	len   uint64
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

func (l List[T]) Iter() Iter[T] {
	return Iter[T]{node: l.first, count: l.len}
}

type Iter[T any] struct {
	node  *Node[T]
	count uint64
}

func (i *Iter[T]) Len() uint64 { return i.count }
func (i *Iter[T]) Valid() bool { return i.count > 0 }
func (i *Iter[T]) Value() T    { return i.node.Value }

func (i *Iter[T]) Move() {
	i.count--
	i.node = i.node.Next
}

func (i *Iter[T]) Next() Iter[T] {
	return Iter[T]{node: i.node.Next, count: i.count - 1}
}

func (i *Iter[T]) Slice(to Iter[T]) List[T] {
	if i.count < to.count {
		panic(fmt.Sprintf("incorrect slice: %d %d", i.count, to.count))
	}
	return List[T]{first: i.node, len: i.count - to.count}
}
