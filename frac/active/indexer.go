package active

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"slices"
	"unsafe"

	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/util"
)

const uint32Size = uint32(unsafe.Sizeof(uint32(0)))

// Indexer indexes documents with concurrency limitation
type Indexer struct {
	workerPool WorkerLimiter
}

// NewIndexer creates a new indexer with specified number of workers
func NewIndexer(workerPool WorkerLimiter) *Indexer {
	return &Indexer{
		workerPool: workerPool,
	}
}

// indexerBuffer is a temporary reusable buffer used during index construction to avoid allocations.
// it holds intermediate data structures that are needed during processing but not in the final index.
type indexerBuffer struct {
	sizes     []uint32
	fields    []string
	fieldTIDs []uint32
	tokenMap  map[tokenStr]uint32
}

// Index starts asynchronous document indexing
func (idx *Indexer) Index(block storage.DocBlock, apply func(index *memIndex, err error)) {
	idx.workerPool.Acquire()
	go func() {
		apply(NewMemIndex(block))
		idx.workerPool.Release()
	}()
}

// NewMemIndex creates an in-memory index from a document block
func NewMemIndex(data storage.DocBlock) (*memIndex, error) {
	sw := stopwatch.New()

	tmp, release := NewResources()
	defer release()

	// decompress metadata
	payload, err := decompressMeta(tmp, data, sw)
	if err != nil {
		return nil, err
	}

	buf := tmp.GetBuffer()

	// decode metadata
	meta, err := decodeMetadata(tmp, buf, payload, sw)
	if err != nil {
		return nil, err
	}
	// initialize index
	idx := newMemIndex()
	idx.docsCount = uint32(len(meta))
	idx.ids = idx.res.GetIDs(len(meta))
	idx.positions = idx.res.GetDocPosSlice(len(meta))
	idx.blocksOffsets = idx.res.GetUint64s(1) // only one block per bulk
	idx.blocksOffsets[0] = data.GetExt2()

	// extract tokens from metadata
	tids, lids, tokens, err := extractTokens(idx, tmp, buf, meta)
	if err != nil {
		return nil, err
	}

	// group documents by token
	tokenLIDs := groupLIDsByTID(idx, tmp, tids, lids, len(tokens))

	// organize tokens and fields
	organizeTokens(idx, tmp, buf, tokens, tokenLIDs)

	return idx, nil
}

// tokenStr represents a unique token as a (field, value) pair.
// used as a map key during token deduplication.
type tokenStr struct {
	field string
	value string
}

func toToken(k, v []byte) tokenStr {
	return tokenStr{
		field: util.ByteToStringUnsafe(k),
		value: util.ByteToStringUnsafe(v),
	}
}

// extractTokens extracts tokens from document metadata
func extractTokens(
	idx *memIndex,
	tmp *Resources,
	buf *indexerBuffer,
	meta []indexer.MetaData,
) ([]uint32, []uint32, []tokenStr, error) {
	var docOffset uint64
	var totalTokens uint32

	// calculate document positions in the original block
	// each document is stored as: [size: uint32][data: size bytes]
	positions := tmp.GetDocPosSlice(len(meta))
	prev := seq.PackDocPos(0, docOffset)

	for i := range meta {
		docMeta := meta[i]
		if docMeta.Size > 0 {
			prev = seq.PackDocPos(0, docOffset)
			docOffset += uint64(docMeta.Size) + uint64(uint32Size)
		}
		positions[i] = prev
		totalTokens += docMeta.TokensCount()
	}

	// create ordering by document ID (descending)
	// we need to map global document IDs to local IDs (LIDs)
	order := tmp.GetUint32s(len(meta))
	for i := range order {
		order[i] = uint32(i)
	}
	slices.SortFunc(order, func(a, b uint32) int {
		return seq.Compare(meta[b].ID, meta[a].ID)
	})

	hash := fnv.New64a()
	var idBinary [16]byte

	// fill index structures with sorted documents
	for i, origIdx := range order {
		docMeta := meta[origIdx]
		idx.ids[i] = docMeta.ID
		idx.positions[i] = positions[origIdx]
		idx.docsSize += uint64(docMeta.Size)
		hash.Write(docMeta.ID.AppendBinary(idBinary[:0]))
	}
	idx.hash = hash.Sum64()

	// allocate slices for token-document relationships
	lids := tmp.GetUint32s(int(totalTokens))[:0] // local document ID for each token occurrence
	tids := tmp.GetUint32s(int(totalTokens))[:0] // token ID for each occurrence

	buf.tokenMap[tokenStr{field: seq.TokenAll}] = 0 // reserve ALL token (just for proper sealing)

	// process documents in ID-sorted order
	for i, origIdx := range order {
		docMeta := meta[origIdx]

		lid := uint32(i + 1)

		err := docMeta.DecodeTokens(func(k, v []byte) error {
			if bytes.Equal(k, seq.AllTokenName) {
				return nil
			}
			token := toToken(k, v)
			tid, exists := buf.tokenMap[token]
			if !exists {
				tid = uint32(len(buf.tokenMap)) // assign new token ID
				buf.tokenMap[token] = tid
			}
			tids = append(tids, tid)
			lids = append(lids, lid) // store lid+1 (1-based indexing for internal use)
			return nil
		})

		if err != nil {
			return nil, nil, nil, err
		}
	}

	// create reverse mapping: tokenID -> tokenKey
	tokens := tmp.GetTokens(len(buf.tokenMap))
	for key, tokenID := range buf.tokenMap {
		tokens[tokenID] = key
	}

	return tids, lids, tokens, nil
}

// groupLIDsByTID groups document IDs by token
// input: flat arrays of (tid, lid) pairs
// output: 2D array where tokenLIDs[tid] = []lid
func groupLIDsByTID(idx *memIndex, tmp *Resources, tids, lids []uint32, tokenCount int) [][]uint32 {
	// phase 1: count documents per token
	counts := tmp.GetUint32s(tokenCount)
	clear(counts)
	for _, tid := range tids {
		counts[tid]++
	}

	// phase 2: allocate slices for each token group
	// we use a single large buffer and slice it for efficiency
	tokenLIDs := tmp.GetUint32Slices(tokenCount)
	allTokenLIDs := idx.res.GetUint32s(len(lids))
	idx.allTokenLIDsCount = len(lids)

	tokenLIDs = tokenLIDs[:len(counts)]
	for tid, count := range counts {
		tokenLIDs[tid] = allTokenLIDs[:count][:0]
		allTokenLIDs = allTokenLIDs[count:]
	}

	// phase 3: populate groups with LIDs
	lids = lids[:len(tids)]
	for i, tid := range tids {
		if len(tokenLIDs[tid]) > 0 {
			if lids[i] == lastLID(tokenLIDs[tid]) {
				// tokens deduplication (the same token can occurs a few times for one doc)
				idx.allTokenLIDsCount--
				continue
			}
		}
		tokenLIDs[tid] = append(tokenLIDs[tid], lids[i])
	}

	return tokenLIDs
}

func lastLID(s []uint32) uint32 {
	return s[len(s)-1]
}

// organizeTokens organizes tokens and fields in the index with proper sorting
func organizeTokens(idx *memIndex, tmp *Resources, buf *indexerBuffer, tokens []tokenStr, tokenLIDs [][]uint32) {
	tokenSize := 0
	order := tmp.GetUint32s(len(tokens))
	order = order[:len(tokens)]
	for i, t := range tokens {
		order[i] = uint32(i)
		tokenSize += len(t.value)
	}

	// create ordering for sorting tokens
	// we'll sort by (field, value) to group tokens by field
	slices.SortFunc(order, func(a, b uint32) int {
		tokenA, tokenB := tokens[a], tokens[b]
		if tokenA.field < tokenB.field {
			return -1
		}
		if tokenA.field == tokenB.field {
			if tokenA.value < tokenB.value {
				return -1
			}
			if tokenA.value == tokenB.value {
				return 0
			}
		}
		return 1
	})

	fieldSize := 0
	prevField := ""

	// prepare buffers for sorted data
	tokenBuffer := idx.res.GetBytes(tokenSize)[:0]
	idx.tokenLIDs = idx.res.GetUint32Slices(len(order))
	idx.tokens = idx.res.GetBytesSlices(len(order))

	// process tokens in sorted order
	for tid, origIdx := range order {
		token := tokens[origIdx]

		// detect field boundaries
		// when field name changes, record the field and its first token position
		if token.field != prevField || prevField == "" {
			fieldSize += len(token.field)
			buf.fields = append(buf.fields, token.field)
			buf.fieldTIDs = append(buf.fieldTIDs, uint32(tid))
		}
		prevField = token.field

		// copy token value to buffer and keep reference
		start := len(tokenBuffer)
		tokenBuffer = append(tokenBuffer, token.value...)

		// store in sorted arrays
		// note: we use original tokenID as index to preserve tokenID->data mapping
		idx.tokens[tid] = tokenBuffer[start:]
		idx.tokenLIDs[tid] = tokenLIDs[origIdx]
	}
	// add sentinel value for easier range calculation
	buf.fieldTIDs = append(buf.fieldTIDs, uint32(len(tokens)))

	// organize fields
	fieldBuffer := idx.res.GetBytes(fieldSize)[:0]
	idx.fields = idx.res.GetBytesSlices(len(buf.fields))

	idx.fieldsTokens = make(map[string]tokenRange, len(buf.fields))

	for i, field := range buf.fields {
		// copy field name to buffer
		start := len(fieldBuffer)
		fieldBuffer = append(fieldBuffer, field...)
		idx.fields[i] = fieldBuffer[start:]

		// calculate token range for this field
		// each field has continuous range of token IDs in sorted order
		startTID := buf.fieldTIDs[i]
		endTID := buf.fieldTIDs[i+1]
		idx.fieldsTokens[util.ByteToStringUnsafe(fieldBuffer[start:])] = tokenRange{
			start: startTID,
			count: endTID - startTID,
		}
	}
}

// decompressMeta decompresses metadata from block
func decompressMeta(res *Resources, block storage.DocBlock, sw *stopwatch.Stopwatch) ([]byte, error) {
	m := sw.Start("decompress_meta")
	defer m.Stop()

	// allocate exact size needed for compressed data
	buffer := res.GetBytes(int(block.RawLen()))
	payload, err := block.DecompressTo(buffer)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// decodeMetadata decodes document metadata from binary format
// format: [size: uint32][data: size bytes][size: uint32][data: size bytes]...
func decodeMetadata(tmp *Resources, buf *indexerBuffer, payload []byte, sw *stopwatch.Stopwatch) ([]indexer.MetaData, error) {
	m := sw.Start("decode_meta")
	defer m.Stop()

	// first pass: scan to determine sizes of each metadata entry
	var offset uint32
	for offset < uint32(len(payload)) {
		size := binary.LittleEndian.Uint32(payload[offset:])
		offset += uint32Size + size
		buf.sizes = append(buf.sizes, size)
	}

	// second pass: decode each metadata entry
	meta := tmp.GetMetadata(len(buf.sizes))
	for i, size := range buf.sizes {
		// skip size field to get to actual data
		data := payload[uint32Size : size+uint32(uint32Size)]
		if err := meta[i].UnmarshalBinaryLazy(data); err != nil {
			return nil, err
		}
		// move to next entry
		payload = payload[size+uint32(uint32Size):]
	}

	return meta, nil
}
