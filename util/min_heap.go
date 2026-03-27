package util

import (
	"cmp"
	"container/heap"
)

// MinHeap is a min‑heap for any comparable type.
// Maintains both a heap structure and a map for fast lookup of items.
type MinHeap[T cmp.Ordered] struct {
	items    []*heapItem[T]     // Heap elements
	indexMap map[T]*heapItem[T] // Value → item mapping for O(1) lookup
}

// heapItem represents an element in the heap.
type heapItem[T comparable] struct {
	value T   // Stored value
	index int // Current index in the heap
}

// NewMinHeap creates and initializes a new MinHeap instance.
func NewMinHeap[T cmp.Ordered]() *MinHeap[T] {
	h := &MinHeap[T]{
		items:    make([]*heapItem[T], 0),
		indexMap: make(map[T]*heapItem[T]),
	}
	heap.Init((*heapWrapper[T])(h))
	return h
}

// Push adds a value to the heap if it doesn't already exist (no duplicates).
func (h *MinHeap[T]) Push(value T) {
	if _, ok := h.indexMap[value]; !ok {
		item := &heapItem[T]{
			value: value,
			index: -1,
		}
		h.indexMap[value] = item
		heap.Push((*heapWrapper[T])(h), item)
	}
}

// Remove deletes one occurrence of the specified value from the heap.
// Does nothing if the value doesn't exist.
func (h *MinHeap[T]) Remove(value T) {
	item, ok := h.indexMap[value]
	if !ok {
		return
	}
	heap.Remove((*heapWrapper[T])(h), item.index)
	delete(h.indexMap, value)
}

// PopMin removes and returns the minimum value from the heap.
// Returns (zero value, false) if the heap is empty.
func (h *MinHeap[T]) PopMin() (T, bool) {
	var zero T
	if len(h.items) == 0 {
		return zero, false
	}
	item := h.items[0]
	value := item.value
	heap.Pop((*heapWrapper[T])(h))
	return value, true
}

// Min returns the minimum value in the heap without removing it.
// Returns (zero value, false) if the heap is empty.
func (h *MinHeap[T]) Min() (T, bool) {
	var zero T
	if len(h.items) == 0 {
		return zero, false
	}
	return h.items[0].value, true
}

// Len returns the current number of elements in the heap.
func (h *MinHeap[T]) Len() int {
	return len(h.items)
}

// heapWrapper is a type alias for MinHeap to implement heap.Interface.
type heapWrapper[T cmp.Ordered] MinHeap[T]

// Len is part of heap.Interface — returns the number of elements.
func (hw *heapWrapper[T]) Len() int {
	return len(hw.items)
}

// Less is part of heap.Interface — defines min‑heap order (smaller values first).
func (hw *heapWrapper[T]) Less(i, j int) bool {
	return hw.items[i].value < hw.items[j].value
}

// Swap is part of heap.Interface — swaps elements and updates their indices.
func (hw *heapWrapper[T]) Swap(i, j int) {
	hw.items[i], hw.items[j] = hw.items[j], hw.items[i]
	hw.items[i].index = i
	hw.items[j].index = j
}

// Push is part of heap.Interface — adds a new element to the heap.
func (hw *heapWrapper[T]) Push(x interface{}) {
	item := x.(*heapItem[T])
	item.index = len(hw.items)
	hw.items = append(hw.items, item)
}

// Pop is part of heap.Interface — removes and returns the last element.
func (hw *heapWrapper[T]) Pop() interface{} {
	old := hw.items
	n := len(old) - 1
	item := old[n]
	item.index = -1
	hw.items = old[0:n]
	return item
}
