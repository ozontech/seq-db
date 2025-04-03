package frac

import (
	"context"

	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
)

const (
	TypeSealed = "sealed"
	TypeActive = "active"
)

// IDsIndex provide access to seq.ID by seq.LID
// where seq.LID (Local ID) is a position of seq.ID in sorted sequence.
// seq.ID sorted in descending order, so for seq.LID1 > seq.LID2
// we have seq.ID1 < seq.ID2
type IDsIndex interface {
	// LessOrEqual checks if seq.ID in LID position less or equal searched seq.ID, i.e. seqID(lid) <= id
	LessOrEqual(lid seq.LID, id seq.ID) bool
	GetMID(seq.LID) seq.MID
	GetRID(seq.LID) seq.RID
	Len() int
}

type DocsIndex interface {
	GetBlocksOffsets(uint32) uint64
	GetDocPos([]seq.ID) []DocPos
	ReadDocs(blockOffset uint64, docOffsets []uint64) ([][]byte, error)
}

type TokenIndex interface {
	GetValByTID(tid uint32) []byte
	GetTIDsByTokenExpr(token parser.Token) ([]uint32, error)
	GetLIDsFromTIDs(tids []uint32, stats lids.Counter, minLID, maxLID uint32, order seq.DocsOrder) []node.Node
}

type SkipIndex interface {
	IsIntersecting(from seq.MID, to seq.MID) bool
	Contains(mid seq.MID) bool
}

type Index interface {
	SkipIndex

	IDsIndex() IDsIndex
	TokenIndex() TokenIndex
	DocsIndex() DocsIndex
}

type IndexProvider interface {
	Indexes() []Index
}

type Fraction interface {
	SkipIndex

	Type() string
	Info() *Info
	TakeIndexProvider(ctx context.Context) (IndexProvider, func())
	FullSize() uint64
	Suicide()
}
