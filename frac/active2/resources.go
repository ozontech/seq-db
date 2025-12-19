package active2

import (
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/resources"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tokenizer"
)

var (
	tokenKeyPool        = resources.NewSizedPool[token](24)
	indexerMetaDataPool = resources.NewSizedPool[indexer.MetaData](24)
	docPosSlicesPool    = resources.NewSizedPool[[]seq.DocPos](24)
	bufPool             = resources.TypedPool[*indexBuffer]{}
	resPool             = resources.TypedPool[*Resources]{}
)

// Resources provides pooled memory allocation for index construction.
// It manages reusable buffers to avoid GC pressure during indexing.
type Resources struct {
	releases *resources.CallStack

	uint32s         resources.SliceOnBytes[uint32]
	uint64s         resources.SliceOnBytes[uint64]
	bytes           resources.SliceAllocator[byte]
	bytesSlices     resources.SliceAllocator[[]byte]
	uint32Slices    resources.SliceAllocator[[]uint32]
	tokenKeys       resources.SliceAllocator[token]
	indexerMetaData resources.SliceAllocator[indexer.MetaData]
	buf             resources.ObjectAllocator[indexBuffer]
	ids             resources.SliceOnBytes[seq.ID]
	docPos          resources.SliceOnBytes[seq.DocPos]
	docPosSlices    resources.SliceAllocator[[]seq.DocPos]
}

func AcquireResources() (*Resources, func()) {
	r, ok := resPool.Get()
	if !ok {
		s := resources.CallStack{}
		r = &Resources{
			releases: &s,

			bytes:           resources.NewBytes(&s),
			uint32s:         resources.NewUint32s(&s),
			uint64s:         resources.NewUint64s(&s),
			uint32Slices:    resources.NewUint32Slices(&s),
			bytesSlices:     resources.NewBytesSlices(&s),
			ids:             resources.NewSliceOnBytes[seq.ID](&s),
			docPos:          resources.NewSliceOnBytes[seq.DocPos](&s),
			docPosSlices:    resources.NewSliceAllocator(&docPosSlicesPool, &s),
			indexerMetaData: resources.NewSliceAllocator(&indexerMetaDataPool, &s),
			tokenKeys:       resources.NewSliceAllocator(&tokenKeyPool, &s),
			buf:             resources.NewObjectAllocator(&bufPool, &s),
		}
	}
	return r, func() {
		r.releases.CallAll()
		resPool.Put(r)
	}
}

func (r *Resources) AllocBytesSlices(s int) [][]byte {
	return r.bytesSlices.AllocSlice(s)
}

func (r *Resources) AllocBytes(s int) []byte {
	return r.bytes.AllocSlice(s)
}

func (r *Resources) AllocUint32s(s int) []uint32 {
	return r.uint32s.AllocSlice(s)
}

func (r *Resources) AllocIDs(s int) []seq.ID {
	return r.ids.AllocSlice(s)
}

func (r *Resources) AllocDocPos(s int) []seq.DocPos {
	return r.docPos.AllocSlice(s)
}

func (r *Resources) AllocDocPosSlices(s int) [][]seq.DocPos {
	return r.docPosSlices.AllocSlice(s)
}

func (r *Resources) AllocUint64s(s int) []uint64 {
	return r.uint64s.AllocSlice(s)
}

func (r *Resources) AllocUint32Slices(s int) [][]uint32 {
	return r.uint32Slices.AllocSlice(s)
}

func (r *Resources) AllocMetadata(s int) []indexer.MetaData {
	return r.indexerMetaData.AllocSlice(s)
}

func (r *Resources) AllocTokens(s int) []token {
	return r.tokenKeys.AllocSlice(s)
}

func (r *Resources) Buffer() *indexBuffer {
	return r.buf.Alloc(func() *indexBuffer {
		return &indexBuffer{
			sizes:     make([]uint32, 0, 1000),
			fields:    make([]string, 0, 100),
			fieldTIDs: make([]uint32, 0, 100),
			tokens:    make([]tokenizer.MetaToken, 0, 100),
			tokenMap:  make(map[token]uint32, 1000),
		}
	}, func(b *indexBuffer) {
		b.fields = b.fields[:0]
		b.tokens = b.tokens[:0]
		b.fieldTIDs = b.fieldTIDs[:0]
		b.sizes = b.sizes[:0]
		clear(b.tokenMap)
	})
}

// indexBuffer is a temporary buffer used during index construction to avoid allocations.
// It holds intermediate data structures that are needed during processing but not in the final index.
type indexBuffer struct {
	sizes     []uint32
	fields    []string
	fieldTIDs []uint32
	tokens    []tokenizer.MetaToken
	tokenMap  map[token]uint32
}
