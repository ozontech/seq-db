package active

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"slices"
	"unsafe"

	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/tokenizer"
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
// It holds intermediate data structures that are needed during processing but not in the final index.
type indexerBuffer struct {
	sizes     []uint32
	fields    []string
	fieldTIDs []uint32
	tokens    []tokenizer.MetaToken
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
func NewMemIndex(block storage.DocBlock) (*memIndex, error) {
	sw := stopwatch.New()

	res, release := NewResources()
	defer release()

	// Decompress metadata
	payload, err := decompressMeta(res, block, sw)
	if err != nil {
		return nil, err
	}

	buf := res.GetBuffer()

	// Decode metadata
	meta, err := decodeMetadata(res, buf, payload, sw)
	if err != nil {
		return nil, err
	}
	// Initialize index
	idx := newMemIndex()
	idx.docsCount = uint32(len(meta))
	idx.ids = idx.res.GetIDs(len(meta))
	idx.positions = idx.res.GetDocPos(len(meta))
	idx.blocksOffsets = idx.res.GetUint64s(1) // Only one block per bulk
	idx.blocksOffsets[0] = block.GetExt2()

	// Extract tokens from metadata
	tids, lids, tokens, err := extractTokens(idx, res, buf, meta)
	if err != nil {
		return nil, err
	}

	// Group documents by token
	tokenLIDs := groupLIDsByTID(idx, res, tids, lids, len(tokens))

	// Organize tokens and fields
	organizeTokens(idx, res, buf, tokens, tokenLIDs)

	return idx, nil
}

// tokenStr represents a unique token as a (field, value) pair.
// Used as a map key during token deduplication.
type tokenStr struct {
	value string
	field string
}

func toToken(t tokenizer.MetaToken) tokenStr {
	return tokenStr{
		value: util.ByteToStringUnsafe(t.Value),
		field: util.ByteToStringUnsafe(t.Key),
	}
}

// extractTokens extracts tokens from document metadata
func extractTokens(
	idx *memIndex,
	res *Resources,
	buf *indexerBuffer,
	meta []indexer.MetaData,
) ([]uint32, []uint32, []tokenStr, error) {
	var docOffset uint64
	var totalTokens uint32

	// Calculate document positions in the original block
	// Each document is stored as: [size: uint32][data: size bytes]
	positions := res.GetDocPos(len(meta))
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

	// Create ordering by document ID (descending)
	// We need to map global document IDs to local IDs (LIDs)
	order := res.GetUint32s(len(meta))
	for i := range order {
		order[i] = uint32(i)
	}
	slices.SortFunc(order, func(a, b uint32) int {
		return seq.Compare(meta[b].ID, meta[a].ID)
	})

	// Fill index structures with sorted documents
	for lid, origIdx := range order {
		docMeta := meta[origIdx]
		idx.ids[lid] = docMeta.ID
		idx.positions[lid] = positions[origIdx]
		idx.docsSize += uint64(docMeta.Size)
	}

	// Extract and process tokens from all documents
	var err error
	var token tokenStr

	// Allocate slices for token-document relationships
	lids := res.GetUint32s(int(totalTokens))[:0] // Local document ID for each token occurrence
	tids := res.GetUint32s(int(totalTokens))[:0] // Token ID for each occurrence

	// Process documents in ID-sorted order
	for lid, origIdx := range order {
		docMeta := meta[origIdx]

		// Decode tokens for this document
		if buf.tokens, err = docMeta.DecodeTokens(buf.tokens[:0]); err != nil {
			return nil, nil, nil, err
		}

		buf.tokenMap[tokenStr{field: seq.TokenAll}] = 0 // reserve ALL token (just for proper sealing)

		// Process each token in the document
		for _, t := range buf.tokens {
			if bytes.Equal(t.Key, seq.AllTokenName) {
				continue
			}
			token = toToken(t)
			tid, exists := buf.tokenMap[token]
			if !exists {
				tid = uint32(len(buf.tokenMap)) // assign new token ID
				buf.tokenMap[token] = tid
			}
			tids = append(tids, tid)
			lids = append(lids, uint32(lid)+1) // store lid+1 (1-based indexing for internal use)
		}
	}

	// Create reverse mapping: tokenID -> tokenKey
	tokens := res.GetTokens(len(buf.tokenMap))
	for key, tokenID := range buf.tokenMap {
		tokens[tokenID] = key
	}

	return tids, lids, tokens, nil
}

// groupLIDsByTID groups document IDs by token
// Input: flat arrays of (tid, lid) pairs
// Output: 2D array where tokenLIDs[tid] = []lid
func groupLIDsByTID(idx *memIndex, res *Resources, tids, lids []uint32, tokenCount int) [][]uint32 {
	// Phase 1: Count documents per token
	counts := res.GetUint32s(tokenCount)
	clear(counts)
	for _, tid := range tids {
		counts[tid]++
	}

	// Phase 2: Allocate slices for each token group
	// We use a single large buffer and slice it for efficiency
	tokenLIDs := res.GetUint32Slices(tokenCount)
	allTokenLIDs := idx.res.GetUint32s(len(lids))
	idx.allTokenLIDsCount = len(lids)

	tokenLIDs = tokenLIDs[:len(counts)]
	for tid, count := range counts {
		tokenLIDs[tid] = allTokenLIDs[:count][:0]
		allTokenLIDs = allTokenLIDs[count:]
	}

	// Phase 3: Populate groups with LIDs
	lids = lids[:len(tids)]
	for i, tid := range tids {
		if len(tokenLIDs[tid]) > 0 {
			if lids[i] == lastLID(tokenLIDs[tid]) { // deduplication
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
func organizeTokens(idx *memIndex, res *Resources, buf *indexerBuffer, tokens []tokenStr, tokenLIDs [][]uint32) {
	tokenSize := 0
	order := res.GetUint32s(len(tokens))
	order = order[:len(tokens)]
	for i, t := range tokens {
		order[i] = uint32(i)
		tokenSize += len(t.value)
	}

	// Create ordering for sorting tokens
	// We'll sort by (field, value) to group tokens by field
	slices.SortFunc(order, func(a, b uint32) int {
		tokenA, tokenB := tokens[a], tokens[b]
		return cmp.Or(
			cmp.Compare(tokenA.field, tokenB.field),
			cmp.Compare(tokenA.value, tokenB.value),
		)
	})

	fieldSize := 0
	prevField := ""

	// Prepare buffers for sorted data
	tokenBuffer := idx.res.GetBytes(tokenSize)[:0]
	idx.tokenLIDs = idx.res.GetUint32Slices(len(order))
	idx.tokens = idx.res.GetBytesSlices(len(order))

	// Process tokens in sorted order
	for tid, origIdx := range order {
		token := tokens[origIdx]

		// Detect field boundaries
		// When field name changes, record the field and its first token position
		if token.field != prevField || prevField == "" {
			fieldSize += len(token.field)
			buf.fields = append(buf.fields, token.field)
			buf.fieldTIDs = append(buf.fieldTIDs, uint32(tid))
		}
		prevField = token.field

		// Copy token value to buffer and keep reference
		start := len(tokenBuffer)
		tokenBuffer = append(tokenBuffer, token.value...)

		// Store in sorted arrays
		// Note: We use original tokenID as index to preserve tokenID->data mapping
		idx.tokens[tid] = tokenBuffer[start:]
		idx.tokenLIDs[tid] = tokenLIDs[origIdx]
	}
	// Add sentinel value for easier range calculation
	buf.fieldTIDs = append(buf.fieldTIDs, uint32(len(tokens)))

	// Organize fields
	fieldBuffer := idx.res.GetBytes(fieldSize)[:0]
	idx.fields = idx.res.GetBytesSlices(len(buf.fields))

	idx.fieldsTokens = make(map[string]tokenRange, len(buf.fields))

	for i, field := range buf.fields {
		// Copy field name to buffer
		start := len(fieldBuffer)
		fieldBuffer = append(fieldBuffer, field...)
		idx.fields[i] = fieldBuffer[start:]

		// Calculate token range for this field
		// Each field has continuous range of token IDs in sorted order
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

	// Allocate exact size needed for compressed data
	buffer := res.GetBytes(int(block.RawLen()))
	payload, err := block.DecompressTo(buffer)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// decodeMetadata decodes document metadata from binary format
// Format: [size: uint32][data: size bytes][size: uint32][data: size bytes]...
func decodeMetadata(res *Resources, buf *indexerBuffer, payload []byte, sw *stopwatch.Stopwatch) ([]indexer.MetaData, error) {
	m := sw.Start("decode_meta")
	defer m.Stop()

	// First pass: scan to determine sizes of each metadata entry
	var offset uint32
	for offset < uint32(len(payload)) {
		size := binary.LittleEndian.Uint32(payload[offset:])
		offset += uint32Size + size
		buf.sizes = append(buf.sizes, size)
	}

	// Second pass: decode each metadata entry
	meta := res.GetMetadata(len(buf.sizes))
	for i, size := range buf.sizes {
		// Skip size field to get to actual data
		data := payload[uint32Size : size+uint32(uint32Size)]
		if err := meta[i].UnmarshalBinaryLazy(data); err != nil {
			return nil, err
		}
		// Move to next entry
		payload = payload[size+uint32(uint32Size):]
	}

	return meta, nil
}
