package node

var _ LIDBatch = &rangeBatch{}

func newRangeBatch(minLID, maxLID uint32) LIDBatch {
	if minLID > maxLID {
		return EmptyBatch()
	}
	return &rangeBatch{min: minLID, max: maxLID}
}

type rangeBatch struct {
	min, max uint32
}

func (b *rangeBatch) IsEmpty() bool {
	return false
}

func (b *rangeBatch) Len() int {
	return int(uint64(b.max) - uint64(b.min) + 1)
}

func (b *rangeBatch) Min() uint32 {
	return b.min
}

func (b *rangeBatch) Max() uint32 {
	return b.max
}

func (b *rangeBatch) ManyIter(asc bool) ManyIter {
	return newRangeCursor(b.min, b.max, asc)
}

func (b *rangeBatch) Iter() Iter {
	return newRangeCursor(b.min, b.max, true)
}

func (b *rangeBatch) ReverseIter() Iter {
	return newRangeCursor(b.min, b.max, false)
}

func (b *rangeBatch) Narrow(minLID, maxLID uint32) LIDBatch {
	return newRangeBatch(max(b.min, minLID), min(b.max, maxLID))
}

type rangeCursor struct {
	next      uint32
	remaining uint64
	asc       bool
}

func newRangeCursor(minLID, maxLID uint32, asc bool) *rangeCursor {
	next := minLID
	if !asc {
		next = maxLID
	}
	return &rangeCursor{
		next:      next,
		remaining: uint64(maxLID) - uint64(minLID) + 1,
		asc:       asc,
	}
}

func (c *rangeCursor) Next() (uint32, bool) {
	start, n := c.take(1)
	return start, n != 0
}

func (c *rangeCursor) NextGeq(geq uint32) (uint32, bool) {
	if c.remaining == 0 {
		return 0, false
	}
	if c.asc && c.next < geq {
		c.skip(uint64(geq) - uint64(c.next))
	} else if !c.asc && c.next > geq {
		c.skip(uint64(c.next) - uint64(geq))
	}
	return c.Next()
}

func (c *rangeCursor) CopyLIDs(dst []LID, tmp []uint32) int {
	start, n := c.take(min(len(dst), len(tmp)))
	for i := 0; i < n; i++ {
		if c.asc {
			dst[i] = NewAscLID(start + uint32(i))
		} else {
			dst[i] = NewDescLID(start - uint32(i))
		}
	}
	return n
}

func (c *rangeCursor) CopyRawLIDs(dst []uint32) int {
	start, n := c.take(len(dst))
	for i := 0; i < n; i++ {
		if c.asc {
			dst[i] = start + uint32(i)
		} else {
			dst[i] = start - uint32(i)
		}
	}
	return n
}

func (c *rangeCursor) skip(n uint64) {
	if n >= c.remaining {
		c.remaining = 0
		return
	}
	if c.asc {
		c.next += uint32(n)
	} else {
		c.next -= uint32(n)
	}
	c.remaining -= n
}

func (c *rangeCursor) take(limit int) (uint32, int) {
	if limit <= 0 || c.remaining == 0 {
		return 0, 0
	}
	n := min(uint64(limit), c.remaining)
	start := c.next
	c.remaining -= n
	if c.remaining != 0 {
		if c.asc {
			c.next += uint32(n)
		} else {
			c.next -= uint32(n)
		}
	}
	return start, int(n)
}
