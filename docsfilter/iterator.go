package docsfilter

type Iterator struct {
	loader *loader

	minLID uint32
	maxLID uint32

	blockIndex   int
	tryNextBlock bool

	lids []uint32
}

func NewIterator(
	loader *loader,
	minLID uint32,
	maxLID uint32,
) *Iterator {
	return &Iterator{
		loader:       loader,
		minLID:       minLID,
		maxLID:       maxLID,
		tryNextBlock: true,
	}
}
