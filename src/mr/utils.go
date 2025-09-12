package mr

import (
	"sync"
)

type syncQ[T comparable] struct {
	mu sync.Mutex
	sl []T
}

func makeQ[T comparable](n int) *syncQ[T] {
	return &syncQ[T]{
		mu: sync.Mutex{},
		sl: make([]T, 0, n),
	}
}

func (q *syncQ[T]) push(elem ...T) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.sl = append(q.sl, elem...)
}

func (q *syncQ[T]) pop() (elem T, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.sl) > 0 {
		elem = q.sl[0]
		q.sl = q.sl[1:]
		return elem, true
	}
	return elem, false
}

func (q *syncQ[T]) front() (elem T, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.sl) > 0 {
		return q.sl[0], true
	}
	return elem, false
}

func (q *syncQ[T]) list_read() []T {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.sl
}

func (q *syncQ[T]) empty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.sl) == 0
}

func sliceToMap[T any, Slice []T](slice Slice) map[int]T {
	m := make(map[int]T, len(slice))
	for idx, elem := range slice {
		m[idx] = elem
	}
	return m
}
