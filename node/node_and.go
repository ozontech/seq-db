package node

import (
	"fmt"
)

// nodeAnd implements Node (single LID only).
type nodeAnd struct {
	less     LessFn
	left     Node
	right    Node
	leftID   uint32
	hasLeft  bool
	rightID  uint32
	hasRight bool
}

func (n *nodeAnd) String() string {
	return fmt.Sprintf("(%s AND %s)", n.left.String(), n.right.String())
}

func NewAnd(left, right Node, reverse bool) *nodeAnd {
	node := &nodeAnd{
		less:  GetLessFn(reverse),
		left:  left,
		right: right,
	}
	node.leftID, node.hasLeft = node.left.Next()
	node.rightID, node.hasRight = node.right.Next()
	return node
}

func (n *nodeAnd) readLeft() {
	n.leftID, n.hasLeft = n.left.Next()
}

func (n *nodeAnd) readRight() {
	n.rightID, n.hasRight = n.right.Next()
}

func (n *nodeAnd) readLeftGeq(minLID uint32) {
	n.leftID, n.hasLeft = n.left.NextGeq(minLID)
}

func (n *nodeAnd) readRightGeq(minLID uint32) {
	n.rightID, n.hasRight = n.right.NextGeq(minLID)
}

func (n *nodeAnd) Next() (uint32, bool) {
	for n.hasLeft && n.hasRight && n.leftID != n.rightID {
		for n.hasLeft && n.hasRight && n.less(n.leftID, n.rightID) {
			n.readLeftGeq(n.rightID)
		}
		for n.hasLeft && n.hasRight && n.less(n.rightID, n.leftID) {
			n.readRightGeq(n.leftID)
		}
	}
	if !n.hasLeft || !n.hasRight {
		return 0, false
	}
	cur := n.leftID
	n.readLeft()
	n.readRight()
	return cur, true
}

func (n *nodeAnd) NextGeq(minLID uint32) (uint32, bool) {
	for n.hasLeft && n.hasRight && n.leftID != n.rightID {
		for n.hasLeft && n.hasRight && n.less(n.leftID, n.rightID) {
			n.readLeftGeq(max(minLID, n.rightID))
		}
		for n.hasLeft && n.hasRight && n.less(n.rightID, n.leftID) {
			n.readRightGeq(max(minLID, n.leftID))
		}
	}
	if !n.hasLeft || !n.hasRight {
		return 0, false
	}
	cur := n.leftID
	n.readLeft()
	n.readRight()
	return cur, true
}

// nodeAndBatched implements BatchedNode: batch intersection with Next/NextGeq implemented by draining the output batch.
type nodeAndBatched struct {
	less        LessFn
	left        BatchedNode
	right       BatchedNode
	leftBatch   []uint32
	rightBatch  []uint32
	outBatch    []uint32
	intersectFn func(limit uint32) []uint32
	out         []uint32
	outIdx      int
}

func (n *nodeAndBatched) String() string {
	return fmt.Sprintf("(%s AND %s)", n.left.String(), n.right.String())
}

func NewAndBatched(left, right BatchedNode, reverse bool) BatchedNode {
	node := &nodeAndBatched{
		less:     GetLessFn(reverse),
		left:     left,
		right:    right,
		outBatch: make([]uint32, 0, 4096),
	}
	if reverse {
		node.intersectFn = node.intersectDesc
	} else {
		node.intersectFn = node.intersectAsc
	}
	return node
}

func (n *nodeAndBatched) Next() (uint32, bool) {
	for n.outIdx >= len(n.out) {
		n.out = n.NextBatch(0, 4096)
		n.outIdx = 0
		if len(n.out) == 0 {
			return 0, false
		}
	}
	id := n.out[n.outIdx]
	n.outIdx++
	return id, true
}

func (n *nodeAndBatched) NextGeq(minLID uint32) (uint32, bool) {
	for n.outIdx < len(n.out) && n.out[n.outIdx] < minLID {
		n.outIdx++
	}
	for n.outIdx >= len(n.out) {
		n.out = n.NextBatch(minLID, 4096)
		n.outIdx = 0
		if len(n.out) == 0 {
			return 0, false
		}
	}
	id := n.out[n.outIdx]
	n.outIdx++
	return id, true
}

func (n *nodeAndBatched) NextBatch(minLID uint32, limit uint32) []uint32 {
	for {
		for len(n.leftBatch) == 0 {
			n.leftBatch = n.left.NextBatch(minLID, limit)
			if len(n.leftBatch) == 0 {
				return nil
			}
		}
		for len(n.rightBatch) == 0 {
			n.rightBatch = n.right.NextBatch(minLID, limit)
			if len(n.rightBatch) == 0 {
				return nil
			}
		}
		result := n.intersectFn(limit)
		if len(result) > 0 {
			return result
		}
		// no match in this chunk; force refill on next iteration
		n.leftBatch = nil
		n.rightBatch = nil
	}
}

// gallopSearchAsc finds the smallest index k in arr[low:] where arr[k] >= target.
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
		step <<= 1
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
		step <<= 1
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
		mid := lo + (hi-lo+1)/2
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

func (n *nodeAndBatched) intersectAsc(limit uint32) []uint32 {
	left, right := n.leftBatch, n.rightBatch
	if len(left) == 0 || len(right) == 0 {
		return nil
	}
	n.outBatch = n.outBatch[:0]
	i, j := 0, 0
	for i < len(left) && j < len(right) && uint32(len(n.outBatch)) < limit {
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
	n.leftBatch = left[i:]
	n.rightBatch = right[j:]
	return n.outBatch
}

func (n *nodeAndBatched) intersectDesc(limit uint32) []uint32 {
	left, right := n.leftBatch, n.rightBatch
	if len(left) == 0 || len(right) == 0 {
		return nil
	}
	n.outBatch = n.outBatch[:0]
	i, j := len(left)-1, len(right)-1
	for i >= 0 && j >= 0 && uint32(len(n.outBatch)) < limit {
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
	n.leftBatch = left[:i+1]
	n.rightBatch = right[:j+1]
	return n.outBatch
}

func max(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}
