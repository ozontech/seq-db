package node

import (
	"fmt"
	"math"
)

type nodeAnd struct {
	left  Node
	right Node

	leftBatch  []uint32
	rightBatch []uint32

	// temporary batch for pushing up lids. Should have some pool dedicated only to the current search request
	outBatch []uint32

	intersectFn func() []uint32
}

func (n *nodeAnd) String() string {
	return fmt.Sprintf("(%s AND %s)", n.left.String(), n.right.String())
}

func NewAnd(left, right Node, reverse bool) *nodeAnd {
	node := &nodeAnd{
		left:  left,
		right: right,
	}
	node.outBatch = make([]uint32, 0, 4096)
	if reverse {
		// reverse is order asc for query, we intersect batches sorted in reverse order
		node.intersectFn = node.intersectDesc
	} else {
		// reverse is order asc for query, we intersect batches sorted in reverse order
		node.intersectFn = node.intersectAsc
	}
	node.leftBatch = node.left.Next(math.MaxUint32)
	node.rightBatch = node.right.Next(math.MaxUint32)
	return node
}

// gallopSearchAsc finds the smallest index k in arr[low:] where arr[k] >= target.
// TODO replace with shotgun intersection
func gallopSearchAsc(arr []uint32, low int, target uint32) int {
	if low >= len(arr) {
		return len(arr)
	}
	if arr[len(arr)-1] < target {
		return len(arr)
	}
	if arr[low] >= target {
		return low
	}

	step := 1
	pos := low + step
	for pos < len(arr) && arr[pos] < target {
		step <<= 1 // double the step
		pos = low + step
	}

	lo := low + (step >> 1)
	hi := pos
	if hi >= len(arr) {
		hi = len(arr) - 1
	}

	for lo < hi {
		mid := lo + (hi-lo)/2
		if arr[mid] >= target {
			hi = mid
		} else {
			lo = mid + 1
		}
	}

	return lo
}

// intersectAsc intersects two batches sorted in ascending order, iterating forward.
// TODO takes 150us for ~10k-20k batches. can we do better?
// TODO replace with shotgun intersection
func (n *nodeAnd) intersectAsc() []uint32 {
	left, right := n.leftBatch, n.rightBatch
	if len(left) == 0 || len(right) == 0 {
		return nil
	}

	n.outBatch = n.outBatch[:0]

	i, j := 0, 0
	for i < len(left) && j < len(right) {
		if left[i] == right[j] {
			n.outBatch = append(n.outBatch, left[i])
			i++
			j++
		} else if left[i] < right[j] {
			i = gallopSearchAsc(left, i+1, right[j])
		} else {
			j = gallopSearchAsc(right, j+1, left[i])
		}
	}

	// trim batches. the "leftover" will be from one side only, the other one shall be empty
	n.leftBatch = left[i:]
	n.rightBatch = right[j:]

	return n.outBatch
}

func gallopSearchDesc(arr []uint32, high int, target uint32) int {
	if high < 0 || arr[0] > target {
		return -1
	}
	if arr[high] <= target {
		return high
	}

	step := 1
	pos := high - step
	for pos >= 0 && arr[pos] > target {
		step <<= 1 // double the step
		pos = high - step
	}

	lo := pos
	if lo < 0 {
		lo = 0
	}
	hi := high - (step >> 1)
	if hi < 0 {
		hi = 0
	}

	for lo < hi {
		mid := lo + (hi-lo+1)/2 // upper mid to find largest
		if arr[mid] <= target {
			lo = mid
		} else {
			hi = mid - 1
		}
	}

	if arr[lo] <= target {
		return lo
	}
	return -1
}

func (n *nodeAnd) intersectDesc() []uint32 {
	left, right := n.leftBatch, n.rightBatch
	if len(left) == 0 || len(right) == 0 {
		return nil
	}

	n.outBatch = n.outBatch[:0]

	i, j := len(left)-1, len(right)-1
	for i >= 0 && j >= 0 {
		if left[i] == right[j] {
			n.outBatch = append(n.outBatch, left[i])
			i--
			j--
		} else if left[i] > right[j] {
			i = gallopSearchDesc(left, i-1, right[j])
		} else {
			j = gallopSearchDesc(right, j-1, left[i])
		}
	}

	// trim batches. the "leftover" will be from one side only, the other one shall be empty
	n.leftBatch = left[:i+1]
	n.rightBatch = right[:j+1]

	return n.outBatch
}

// TODO limit is ignored
func (n *nodeAnd) Next(limit uint32) []uint32 {
	for {
		if len(n.leftBatch) == 0 {
			n.leftBatch = n.left.Next(math.MaxUint32)
		}
		if len(n.rightBatch) == 0 {
			n.rightBatch = n.right.Next(math.MaxUint32)
		}

		if len(n.leftBatch) == 0 || len(n.rightBatch) == 0 {
			return nil
		}

		result := n.intersectFn()

		if len(result) > 0 {
			return result
		}
	}
}

// TODO limit is ignored
func (n *nodeAnd) NextGeq(minLID uint32, limit uint32) []uint32 {
	for {
		if len(n.leftBatch) == 0 {
			n.leftBatch = n.left.NextGeq(minLID, limit)
		}
		if len(n.rightBatch) == 0 {
			n.rightBatch = n.right.NextGeq(minLID, limit)
		}

		if len(n.leftBatch) == 0 || len(n.rightBatch) == 0 {
			return nil
		}

		result := n.intersectFn()

		if len(result) > 0 {
			return result
		}
	}
}
