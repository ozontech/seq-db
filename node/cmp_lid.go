package node

import "math"

// CmpLID is an encoded representation of LID and reverse flag made specifically for fast compare operations.
//
// For reverse order LID is inverted as follows: "MaxUint32 - LID" formula using XOR mask. Terminal LID value is 0 instead
// of MaxUint32 in reverse order, but 0 is XORed to MaxUint32. Which means, null value will always have lid field set to
// 0xFFFFFFFF (math.MaxUint32) regardless of reverse (order) flag.
type CmpLID struct {
	lid  uint32 // do not read this field, use Unpack instead
	mask uint32
}

func NullCmpLID() CmpLID {
	// reverse flag does not matter, as null values are never unpacked
	return NewCmpLID(math.MaxUint32, false)
}

// NewCmpLIDOrderDesc returns LIDs for desc sort order
func NewCmpLIDOrderDesc(lid uint32) CmpLID {
	return CmpLID{
		lid:  lid,
		mask: uint32(0),
	}
}

// NewCmpLIDOrderAsc returns LIDs for asc sort order
func NewCmpLIDOrderAsc(lid uint32) CmpLID {
	mask := uint32(0xFFFFFFFF)
	return CmpLID{
		lid:  lid ^ mask,
		mask: mask,
	}
}

func NewCmpLID(lid uint32, reverse bool) CmpLID {
	if reverse {
		return NewCmpLIDOrderAsc(lid)
	} else {
		return NewCmpLIDOrderDesc(lid)
	}
}

// Less compares two values. It also does an implicit null check, since we store math.MaxUint32 for null values.
// Which means if we call x.Less(y), then we now for sure that x is not null. Therefore, this Less call can work
// as both "null check + less" combo.
func (c CmpLID) Less(other CmpLID) bool {
	return c.lid < other.lid
}

func (c CmpLID) Inc() CmpLID {
	c.lid++
	return c
}

func (c CmpLID) Eq(other CmpLID) bool {
	return c.lid == other.lid
}

func (c CmpLID) Unpack() uint32 {
	return c.lid ^ c.mask
}

func (c CmpLID) IsNull() bool {
	return c.lid == math.MaxUint32
}
