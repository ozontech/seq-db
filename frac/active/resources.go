package active

import (
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/resources"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tokenizer"
)

var (
	s                   = 24
	tokenKeyPool        = resources.NewSizedPool[tokenStr](s)
	indexerMetaDataPool = resources.NewSizedPool[indexer.MetaData](s)
	docPosSlicesPool    = resources.NewSizedPool[[]seq.DocPos](s)
	bufPool             = resources.TypedPool[*indexerBuffer]{}
	resPool             = resources.TypedPool[*Resources]{}
)

// Resources provides pooled memory allocation for index construction.
// It manages reusable buffers to avoid GC pressure during indexing.
type Resources struct {
	releases *resources.CallStack

	uint32s         *resources.SliceOnBytes[uint32]
	uint64s         *resources.SliceOnBytes[uint64]
	bytes           *resources.SlicesPool[byte]
	bytesSlices     *resources.SlicesPool[[]byte]
	uint32Slices    *resources.SlicesPool[[]uint32]
	tokenKeys       *resources.SlicesPool[tokenStr]
	indexerMetaData *resources.SlicesPool[indexer.MetaData]
	buf             resources.ObjectsPool[indexerBuffer]
	ids             *resources.SliceOnBytes[seq.ID]
	docPos          *resources.SliceOnBytes[seq.DocPos]
	docPosSlices    *resources.SlicesPool[[]seq.DocPos]
}

func NewResources() (*Resources, func()) {
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
			docPosSlices:    resources.NewSlicesPool(docPosSlicesPool, &s),
			indexerMetaData: resources.NewSlicesPool(indexerMetaDataPool, &s),
			tokenKeys:       resources.NewSlicesPool(tokenKeyPool, &s),
			buf:             resources.NewObjectsPool(&bufPool, &s),
		}
	}
	return r, func() {
		r.releases.CallAll()
		resPool.Put(r)
	}
}

func (r *Resources) GetBytesSlices(s int) [][]byte {
	return r.bytesSlices.GetSlice(s)
}

func (r *Resources) GetBytes(s int) []byte {
	return r.bytes.GetSlice(s)
}

func (r *Resources) GetUint32s(s int) []uint32 {
	return r.uint32s.GetSlice(s)
}

func (r *Resources) GetIDs(s int) []seq.ID {
	return r.ids.GetSlice(s)
}

func (r *Resources) GetDocPosSlice(s int) []seq.DocPos {
	return r.docPos.GetSlice(s)
}

func (r *Resources) GetDocPosSlices(s int) [][]seq.DocPos {
	return r.docPosSlices.GetSlice(s)
}

func (r *Resources) GetUint64s(s int) []uint64 {
	return r.uint64s.GetSlice(s)
}

func (r *Resources) GetUint32Slices(s int) [][]uint32 {
	return r.uint32Slices.GetSlice(s)
}

func (r *Resources) GetMetadata(s int) []indexer.MetaData {
	return r.indexerMetaData.GetSlice(s)
}

func (r *Resources) GetTokens(s int) []tokenStr {
	return r.tokenKeys.GetSlice(s)
}

func (r *Resources) GetBuffer() *indexerBuffer {
	return r.buf.Get(func() *indexerBuffer {
		return &indexerBuffer{
			sizes:     make([]uint32, 0, 1000),
			fields:    make([]string, 0, 100),
			fieldTIDs: make([]uint32, 0, 100),
			tokens:    make([]tokenizer.MetaToken, 0, 100),
			tokenMap:  make(map[tokenStr]uint32, 1000),
		}
	}, func(b *indexerBuffer) {
		b.fields = b.fields[:0]
		b.tokens = b.tokens[:0]
		b.fieldTIDs = b.fieldTIDs[:0]
		b.sizes = b.sizes[:0]
		clear(b.tokenMap)
	})
}
