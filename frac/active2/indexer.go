package active2

import (
	"bytes"
	"encoding/binary"
	"unsafe"

	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/seq"
)

type Indexer struct {
	sem chan struct{}
}

func (s *Indexer) Index(meta disk.DocBlock, sw *stopwatch.Stopwatch, fn func(index *memIndex, err error)) {
	s.sem <- struct{}{}
	go func() {
		fn(CreateIndex(meta, sw))
		<-s.sem
	}()
}

func CreateIndex(meta disk.DocBlock, sw *stopwatch.Stopwatch) (*memIndex, error) {
	const uint32Len = uint64(unsafe.Sizeof(uint32(0)))

	docsPos := disk.DocBlock(meta).GetExt2()
	payload, err := disk.DocBlock(meta).DecompressTo(nil)
	if err != nil {
		return nil, err
	}

	index := &memIndex{
		blocksOffsets: []uint64{docsPos},
		positions:     make(map[seq.ID]seq.DocPos),
	}

	fieldsSize := 0
	tokensSize := 0
	totalTokens := 0

	tokenLIDsTotal := 0
	nextDocOffset := uint64(0)

	all := []uint32{}
	ids := []seq.ID{}

	fieldsIndex := map[string]map[string][]uint32{
		seq.TokenAll: {"": {}}, // empty _all_ entry
	}

	for len(payload) > 0 {
		n := binary.LittleEndian.Uint32(payload) + 4
		docMeta := payload[4:n]
		payload = payload[n:]

		var meta frac.MetaData
		if err := meta.UnmarshalBinary(docMeta); err != nil {
			return nil, err
		}

		lid := uint32(len(ids))
		all = append(all, lid)
		ids = append(ids, meta.ID)

		// build token index map
		for _, mt := range meta.Tokens {
			if bytes.Equal(mt.Key, seq.AllTokenName) { // skip if _ALL_ comes from seq-proxy
				tokenLIDsTotal--
				continue
			}
			tokensIndex, ok := fieldsIndex[string(mt.Key)]
			if !ok {
				fieldsSize += len(mt.Key)
				tokensIndex = make(map[string][]uint32)
			}
			tokensLIDs, ok := tokensIndex[string(mt.Value)]
			if !ok {
				totalTokens++
				tokensSize += len(mt.Value)
			}
			tokensIndex[string(mt.Value)] = append(tokensLIDs, lid)
			fieldsIndex[string(mt.Key)] = tokensIndex
		}

		if meta.Size == 0 { // this is a nested document
			continue
		}

		index.positions[meta.ID] = seq.PackDocPos(0, nextDocOffset)
		nextDocOffset += uint64(meta.Size) + uint32Len
		index.docsCount++
	}

	index.docsSize = nextDocOffset - uint64(index.docsCount-1)*uint32Len

	index.fields = make([][]byte, 0, len(fieldsIndex))
	index.tokens = make([][]byte, 0, totalTokens)
	index.tokenLIDs = make([][]uint32, 0, totalTokens)

	lidsData := make([]uint32, 0, tokenLIDsTotal)
	tokensData := make([]byte, 0, tokensSize+fieldsSize)

	inverser := index.sortIDs(all, ids)
	index.sortTokens(fieldsIndex, tokensData, lidsData, inverser)

	index.allTID = uint32(index.fieldsTokens[seq.TokenAll].start)

	return index, nil
}
