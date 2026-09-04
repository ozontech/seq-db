package node

// nodeColumnAgg is a materialized column for aggregation. Size of column is maxLID-minLID+1.
// column[i] has the (source+1) for ith document in search order.
// 0 is zero values which means doc doesn't have a token with the corresponding field.
// It only properly works for keyword aggregation, i.e. each doc has exactly one source/TID.
type nodeColumnAgg struct {
	column []uint64
	minLID uint32 // inclusive
	maxLID uint32 // inclusive
	desc   bool
	cur    uint32 // next cursor position not yet processed, range [minLID, maxLID]
	done   bool
}

func (*nodeColumnAgg) String() string {
	return "COLUMN_AGG"
}

func NewColumnAgg(cursors []BatchedNode, minLID, maxLID uint32, desc bool) Sourced {
	if maxLID < minLID {
		return emptyNodeSourced
	}

	column := make([]uint64, maxLID-minLID+1)
	tmp := make([]uint32, 4*1024)

	for source, cursor := range cursors {
		for {
			batch := cursor.NextBatch()
			if batch.IsEmpty() {
				break
			}
			// we drain all lid lists and do not care about order, hence desc=true
			iter := batch.ManyIter(true)
			for {
				n := iter.CopyRawLIDs(tmp)
				if n == 0 {
					break
				}
				for _, lid := range tmp[:n] {
					if lid < minLID || lid > maxLID {
						continue
					}
					column[lid-minLID] = uint64(source) + 1
				}
			}
		}
	}

	n := &nodeColumnAgg{
		column: column,
		minLID: minLID,
		maxLID: maxLID,
		desc:   desc,
	}
	if desc {
		n.cur = minLID
	} else {
		n.cur = maxLID
	}
	return n
}

func (n *nodeColumnAgg) NextSourced() (LID, uint32) {
	if n.done {
		return NullLID(), 0
	}
	id, source := n.nextGeq(n.cur)
	if id.IsNull() {
		n.done = true
		return id, source
	}
	n.advanceCur(id.Unpack())
	return id, source
}

func (n *nodeColumnAgg) NextSourcedGeq(nextID LID) (LID, uint32) {
	if n.done {
		return NullLID(), 0
	}

	nextLID := nextID.Unpack()
	if n.desc {
		if nextLID < n.minLID {
			nextLID = n.minLID
		}
		if nextLID > n.maxLID {
			n.done = true
			return NullLID(), 0
		}
	} else {
		if nextLID > n.maxLID {
			nextLID = n.maxLID
		}
		if nextLID < n.minLID {
			n.done = true
			return NullLID(), 0
		}
	}

	id, source := n.nextGeq(nextLID)
	if id.IsNull() {
		n.done = true
		return id, source
	}
	n.advanceCur(id.Unpack())
	return id, source
}

func (n *nodeColumnAgg) advanceCur(found uint32) {
	if n.desc {
		if found >= n.maxLID {
			n.done = true
			return
		}
		n.cur = found + 1
		return
	}
	if found <= n.minLID {
		n.done = true
		return
	}
	n.cur = found - 1
}

func (n *nodeColumnAgg) nextGeq(from uint32) (LID, uint32) {
	if n.desc {
		return n.nextGeqDesc(from)
	}

	return n.nextGeqAsc(from)
}

// nextGeqAsc seeks to nextLID, raw lids flow in descending order (i.e. nextLID decreases over time)
func (n *nodeColumnAgg) nextGeqAsc(nextLID uint32) (LID, uint32) {
	if nextLID > n.maxLID {
		nextLID = n.maxLID
	}
	for lid := nextLID; ; lid-- {
		if lid < n.minLID {
			break
		}
		if v := n.column[lid-n.minLID]; v != 0 {
			return NewLID(lid, true), uint32(v - 1)
		}
		if lid == 0 {
			break
		}
	}
	return NullLID(), 0
}

// nextGeqDesc seeks to nextLID, raw lids flow in ascending order (i.e. nextLID increases over time)
func (n *nodeColumnAgg) nextGeqDesc(nextLID uint32) (LID, uint32) {
	if nextLID < n.minLID {
		nextLID = n.minLID
	}
	for lid := nextLID; lid <= n.maxLID; lid++ {
		if v := n.column[lid-n.minLID]; v != 0 {
			return NewLID(lid, false), uint32(v - 1)
		}
	}
	return NullLID(), 0
}
