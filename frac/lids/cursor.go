package lids

type Counter interface {
	AddLIDsCount(int)
}

type Cursor struct {
	table  *Table
	loader *Loader

	minLID uint32
	maxLID uint32

	tid          uint32
	blockIndex   uint32
	tryNextBlock bool

	lids []uint32

	counter Counter
}

func NewLIDsCursor(
	table *Table,
	loader *Loader,
	startIndex uint32,
	tid uint32,
	counter Counter,
	minLID, maxLID uint32,
) *Cursor {
	return &Cursor{
		table:  table,
		loader: loader,

		minLID: minLID,
		maxLID: maxLID,

		tid:          tid,
		blockIndex:   startIndex,
		tryNextBlock: true,

		counter: counter,
	}
}
