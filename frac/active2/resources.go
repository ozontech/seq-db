package active2

import (
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/resources"
	"github.com/ozontech/seq-db/tokenizer"
)

var (
	tokenKeyPool        = resources.NewSizedPool[token](24)
	indexerMetaDataPool = resources.NewSizedPool[indexer.MetaData](24)
	tokenMapPool        = resources.TypedPool[map[token]uint32]{}
	resourcesPool       = resources.TypedPool[*Resources]{}
	bufPool             = resources.TypedPool[*indexBuffer]{}
)

// Resources provides pooled memory allocation for index construction.
// It manages reusable buffers to avoid GC pressure during indexing.
type Resources struct {
	releases *resources.CallStack

	uint32s         resources.SliceOnBytes[uint32]
	uint64s         resources.SliceOnBytes[uint64]
	bytes           resources.SliceAllocator[byte]
	uint32Slices    resources.SliceAllocator[[]uint32]
	tokenKeys       resources.SliceAllocator[token]
	indexerMetaData resources.SliceAllocator[indexer.MetaData]
	tokenMap        resources.MapAllocator[token, uint32]
	buf             resources.ObjectAllocator[indexBuffer]
}

func NewResources() (*Resources, func()) {
	r, ok := resourcesPool.Get()
	if !ok {
		s := resources.CallStack{}
		r = &Resources{
			releases:        &s,
			uint32s:         resources.NewUint32s(&s),
			uint64s:         resources.NewUint64s(&s),
			bytes:           resources.NewBytes(&s),
			uint32Slices:    resources.NewUint32Slices(&s),
			indexerMetaData: resources.NewSliceAllocator(&indexerMetaDataPool, &s),
			tokenKeys:       resources.NewSliceAllocator(&tokenKeyPool, &s),
			tokenMap:        resources.NewMapAllocator(&tokenMapPool, &s),
			buf:             resources.NewObjectAllocator(&bufPool, &s),
		}
	}
	return r, func() {
		r.releases.CallAll()
		resourcesPool.Put(r)
	}
}

func (r *Resources) Bytes() resources.SliceAllocator[byte] {
	return r.bytes
}

func (r *Resources) Uint32s() resources.SliceOnBytes[uint32] {
	return r.uint32s
}

func (r *Resources) Uint64s() resources.SliceOnBytes[uint64] {
	return r.uint64s
}

func (r *Resources) Uint32Slices() resources.SliceAllocator[[]uint32] {
	return r.uint32Slices
}

func (r *Resources) Metadata() resources.SliceAllocator[indexer.MetaData] {
	return r.indexerMetaData
}

func (r *Resources) Tokens() resources.SliceAllocator[token] {
	return r.tokenKeys
}

func (r *Resources) TokenMap() resources.MapAllocator[token, uint32] {
	return r.tokenMap
}

func (r *Resources) Buffer() *indexBuffer {
	return r.buf.Alloc(func() *indexBuffer {
		return &indexBuffer{
			sizes:     make([]uint32, 0, 1000),
			fields:    make([]string, 0, 100),
			fieldTIDs: make([]uint32, 0, 100),
			tokens:    make([]tokenizer.MetaToken, 0, 100),
		}
	}, func(b *indexBuffer) {
		b.fields = b.fields[:0]
		b.tokens = b.tokens[:0]
		b.fieldTIDs = b.fieldTIDs[:0]
		b.sizes = b.sizes[:0]
	})
}

// indexBuffer is a temporary buffer used during index construction to avoid allocations.
// It holds intermediate data structures that are needed during processing but not in the final index.
type indexBuffer struct {
	sizes     []uint32
	fields    []string
	fieldTIDs []uint32
	tokens    []tokenizer.MetaToken
}
