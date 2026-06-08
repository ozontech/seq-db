package fracmanager

import (
	"iter"

	"github.com/ozontech/seq-db/util"
)

// PartitionedCollection manages a collection of objects grouped into partitions by a user‑defined value.
// Each partition is identified by a uint64.
type PartitionedCollection[T any] struct {
	getPartition func(T) uint64          // function to extract partition ID from object
	byKey        map[string]T            // primary index: key -> object
	byPartition  map[uint64]map[string]T // partition ID -> map[key]object
	minPartition *util.MinHeap[uint64]   // min‑heap of partition IDs for O(1) MinPartition
}

// NewPartitionedCollection creates a new empty PartitionedCollection.
func NewPartitionedCollection[T any](getPartition func(T) uint64) PartitionedCollection[T] {
	return PartitionedCollection[T]{
		getPartition: getPartition,
		byKey:        make(map[string]T),
		byPartition:  make(map[uint64]map[string]T),
		minPartition: util.NewMinHeap[uint64](),
	}
}

// Add inserts a new object into the collection.
func (c *PartitionedCollection[T]) Add(key string, obj T) {
	if _, ok := c.byKey[key]; ok {
		return
	}

	partitionID := c.getPartition(obj)
	if _, ok := c.byPartition[partitionID]; !ok {
		c.minPartition.Push(partitionID)
		c.byPartition[partitionID] = make(map[string]T)
	}
	c.byPartition[partitionID][key] = obj
	c.byKey[key] = obj
}

// Del removes an object from the collection by its key.
// Does nothing if the key doesn't exist.
func (c *PartitionedCollection[T]) Del(key string) {
	obj, ok := c.byKey[key]
	if !ok {
		return
	}

	partitionID := c.getPartition(obj)
	delete(c.byPartition[partitionID], key)
	if len(c.byPartition[partitionID]) == 0 {
		c.minPartition.Remove(partitionID)
		delete(c.byPartition, partitionID)
	}
	delete(c.byKey, key)
}

// MinPartition returns the smallest partition ID among all stored objects.
// Returns 0 if the collection is empty.
func (c *PartitionedCollection[T]) MinPartition() uint64 {
	if val, ok := c.minPartition.Min(); ok {
		return val
	}
	return 0
}

// GetByPartition returns all objects in the specified partition.
func (c *PartitionedCollection[T]) GetByPartition(partitionID uint64) []T {
	partitionMap, ok := c.byPartition[partitionID]
	if !ok {
		return nil
	}
	res := make([]T, 0, len(partitionMap))
	for _, obj := range partitionMap {
		res = append(res, obj)
	}
	return res
}

// Get retrieves an object by its key.
// Returns the object and true if found, zero value and false otherwise.
func (c *PartitionedCollection[T]) Get(key string) (T, bool) {
	obj, ok := c.byKey[key]
	return obj, ok
}

// All returns all objects in the collection.
// The order is not guaranteed.
func (c *PartitionedCollection[T]) All() iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, obj := range c.byKey {
			if !yield(obj) {
				return
			}
		}
	}
}

// Len returns the number of objects in the collection.
func (c *PartitionedCollection[T]) Len() int {
	return len(c.byKey)
}