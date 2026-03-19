package node

import (
	"fmt"
	"math"
)

const (
	descMask = uint32(0)
	ascMask  = uint32(0xFFFFFFFF)
)

// LID is an encoded representation of LID and reverse flag made specifically for fast compare operations.
//
// For reverse order LID is inverted as follows: "MaxUint32 - LID" formula using XOR mask. Terminal LID value is 0 instead
// of MaxUint32 in reverse order, but 0 is XORed to MaxUint32. Which means, null value will always have lid field set to
// 0xFFFFFFFF (math.MaxUint32) regardless of reverse (order) flag.
type LID struct {
	lid  uint32 // do not read this field, use Unpack instead
	mask uint32
}

func NullLID() LID {
	// order does not matter, as null values are never unpacked
	return NewDescLID(math.MaxUint32)
}

// NewDescLID returns LIDs for desc sort order
func NewDescLID(lid uint32) LID {
	return LID{
		lid:  lid,
		mask: descMask,
	}
}

// NewAscLID returns LIDs for asc sort order
func NewAscLID(lid uint32) LID {
	return LID{
		lid:  lid ^ ascMask,
		mask: ascMask,
	}
}

func NewLID(lid uint32, reverse bool) LID {
	if reverse {
		return NewAscLID(lid)
	} else {
		return NewDescLID(lid)
	}
}

// Less compares two values. It also does an implicit null check, since we store math.MaxUint32 for null values.
// Which means if we call x.Less(y), then we know for sure that x is not null. Therefore, this Less call can work
// as both "null check + less" combo.
func (c LID) Less(other LID) bool {
	return c.lid < other.lid
}

func (c LID) LessOrEq(other LID) bool {
	return c.lid <= other.lid
}

func (c LID) Inc() LID {
	c.lid++
	return c
}

func (c LID) Eq(other LID) bool {
	return c.lid == other.lid
}

func Max(left LID, right LID) LID {
	if left.lid > right.lid {
		return left
	} else {
		return right
	}
}

func Min(left LID, right LID) LID {
	if left.lid < right.lid {
		return left
	} else {
		return right
	}
}

func (c LID) Unpack() uint32 {
	return c.lid ^ c.mask
}

func (c LID) IsNull() bool {
	return c.lid == math.MaxUint32
}

func (c LID) String() string {
	return fmt.Sprintf("%d, reverse=%t", c.Unpack(), c.mask == ascMask)
}
