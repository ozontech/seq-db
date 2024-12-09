package frac

import (
	"context"

	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/metric/tracer"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
)

const (
	TypeSealed = "sealed"
	TypeActive = "active"
)

// IDsProvider provide access to seq.ID by seq.LID
// where seq.LID (Local ID) is a position of seq.ID in sorted sequence.
// seq.ID sorted in descending order, so for seq.LID1 > seq.LID2
// we have seq.ID1 < seq.ID2
type IDsProvider interface {
	// LessOrEqual checks if seq.ID in LID position less or equal searched seq.ID, i.e. seqID(lid) <= id
	LessOrEqual(lid seq.LID, id seq.ID) bool
	GetMID(seq.LID) seq.MID
	GetRID(seq.LID) seq.RID
	Len() int
}

type DataProvider interface {
	Type() string

	Tracer() *tracer.Tracer
	IDsProvider() IDsProvider
	GetValByTID(tid uint32) []byte
	GetTIDsByTokenExpr(token parser.Token, tids []uint32) ([]uint32, error)
	GetLIDsFromTIDs(tids []uint32, stats lids.Counter, minLID, maxLID uint32, order seq.DocsOrder) []node.Node
	Fetch(ids []seq.ID) ([][]byte, error)
}

type Fraction interface {
	Info() *Info

	IsIntersecting(from seq.MID, to seq.MID) bool
	Contains(mid seq.MID) bool
	FullSize() uint64

	DataProvider(ctx context.Context) (DataProvider, func(), bool)
	Suicide()
}
