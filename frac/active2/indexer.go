package active2

import (
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
	sem chan struct{}
}

// NewIndexer creates a new indexer with specified number of workers
func NewIndexer(workerCount int) *Indexer {
	return &Indexer{
		sem: make(chan struct{}, workerCount),
	}
}

// Index starts asynchronous document indexing
func (idx *Indexer) Index(block storage.DocBlock, apply func(index *memIndex, err error)) {
	idx.sem <- struct{}{}
	go func() {
		apply(NewMemIndex(block))
		<-idx.sem
	}()
}

// NewMemIndex creates an in-memory index from a document block
func NewMemIndex(block storage.DocBlock) (*memIndex, error) {
	sw := stopwatch.New()

	res, cleanup := NewResources()
	defer cleanup()

	// Decompress metadata
	payload, err := decompressMeta(res, block, sw)
	if err != nil {
		return nil, err
	}

	buf := res.Buffer()

	// Decode metadata
	meta, err := decodeMetadata(res, buf, payload, sw)
	if err != nil {
		return nil, err
	}
	// Initialize index
	idx := &memIndex{
		docsCount:     uint32(len(meta)),
		blocksOffsets: []uint64{block.GetExt2()}, // Only one block per bulk
	}

	// Extract tokens from metadata
	tids, lids, tokens, err := extractTokens(res, buf, meta, idx)
	if err != nil {
		return nil, err
	}

	// Group documents by token
	tokenDocGroups := groupLIDsByTID(res, tids, lids, len(tokens))

	// Organize tokens and fields
	organizeTokens(res, buf, idx, tokens, tokenDocGroups)

	// Set special "all" token
	idx.allTID = uint32(idx.fieldsTokens[seq.TokenAll].start)

	return idx, nil
}

// token represents a unique token as a (field, value) pair.
// Used as a map key during token deduplication.
type token struct {
	value string
	field string
}

func toToken(t tokenizer.MetaToken) token {
	return token{
		value: util.ByteToStringUnsafe(t.Value),
		field: util.ByteToStringUnsafe(t.Key),
	}
}

// extractTokens extracts tokens from document metadata
func extractTokens(
	res *Resources,
	buf *indexBuffer,
	meta []indexer.MetaData,
	idx *memIndex,
) ([]uint32, []uint32, []token, error) {
	var totalTokens uint32
	var docOffset uint64

	// Calculate document positions in the original block
	// Each document is stored as: [size: uint32][data: size bytes]
	positions := res.Uint64s().AllocSlice(len(meta))
	prev := seq.PackDocPos(0, docOffset)

	for i := range meta {
		docMeta := meta[i]
		if docMeta.Size > 0 {
			// Start new document group
			prev = seq.PackDocPos(0, docOffset)
			docOffset += uint64(docMeta.Size) + uint64(uint32Size)
		}
		positions[i] = uint64(prev)
		totalTokens += docMeta.TokensCount()
	}

	// Create ordering by document ID (descending)
	// We need to map global document IDs to local IDs (LIDs)
	order := res.Uint32s().AllocSlice(len(meta))
	for i := range order {
		order[i] = uint32(i)
	}
	slices.SortFunc(order, func(a, b uint32) int {
		return seq.Compare(meta[b].ID, meta[a].ID)
	})

	// Fill index structures with sorted documents
	ids := make([]seq.ID, len(order))
	pos := make([]seq.DocPos, len(order))

	for lid, origIdx := range order {
		docMeta := meta[origIdx]
		ids[lid] = docMeta.ID
		idx.docsSize += uint64(docMeta.Size)
		pos[lid] = seq.DocPos(positions[origIdx])
	}

	idx.ids = ids
	idx.positions = pos

	// Extract and process tokens from all documents
	var err error
	var token token

	// Allocate slices for token-document relationships
	lids := res.Uint32s().AllocSlice(int(totalTokens))[:0] // Local document ID for each token occurrence
	tids := res.Uint32s().AllocSlice(int(totalTokens))[:0] // Token ID for each occurrence

	// Map tokenKey -> tokenID (global token identifier)
	tokenMap := res.TokenMap().Alloc(1000)

	// Process documents in ID-sorted order
	for lid, origIdx := range order {
		docMeta := meta[origIdx]

		// Decode tokens for this document
		if buf.tokens, err = docMeta.DecodeTokens(buf.tokens[:0]); err != nil {
			return nil, nil, nil, err
		}

		// Process each token in the document
		for _, t := range buf.tokens {
			token = toToken(t)
			tid, exists := tokenMap[token]
			if !exists {
				tid = uint32(len(tokenMap)) // assign new token ID
				tokenMap[token] = tid
			}
			tids = append(tids, tid)
			lids = append(lids, uint32(lid)+1) // store lid+1 (1-based indexing for internal use)
		}
	}

	// Create reverse mapping: tokenID -> tokenKey
	tokens := res.Tokens().AllocSlice(len(tokenMap))
	for key, tokenID := range tokenMap {
		tokens[tokenID] = key
	}

	return tids, lids, tokens, nil
}

// groupLIDsByTID groups document IDs by token
// Input: flat arrays of (tid, lid) pairs
// Output: 2D array where tokenLIDs[tid] = []lid
func groupLIDsByTID(res *Resources, tids, lids []uint32, tokenCount int) [][]uint32 {
	// Phase 1: Count documents per token
	counts := res.Uint32s().AllocSlice(tokenCount)
	clear(counts)
	for _, tid := range tids {
		counts[tid]++
	}

	// Phase 2: Allocate slices for each token group
	// We use a single large buffer and slice it for efficiency
	tokenLIDs := res.Uint32Slices().AllocSlice(tokenCount)
	buffer := make([]uint32, len(lids))

	tokenLIDs = tokenLIDs[:len(counts)]
	for tid, count := range counts {
		tokenLIDs[tid] = buffer[:count][:0]
		buffer = buffer[count:]
	}

	// Phase 3: Populate groups with document IDs
	// We reuse docIDs slice bounds for safety
	lids = lids[:len(tids)]
	for i, tid := range tids {
		tokenLIDs[tid] = append(tokenLIDs[tid], lids[i])
	}

	return tokenLIDs
}

// organizeTokens organizes tokens and fields in the index with proper sorting
func organizeTokens(res *Resources, buf *indexBuffer, idx *memIndex, tokens []token, tokenLIDs [][]uint32) {
	tokenSize := 0
	order := res.Uint32s().AllocSlice(len(tokens))
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
	tokenBuffer := make([]byte, 0, tokenSize)
	sortedTokens := make([][]byte, len(order))
	sortedTokenLIDs := make([][]uint32, len(order))

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
		sortedTokens[tid] = tokenBuffer[start:]
		sortedTokenLIDs[tid] = tokenLIDs[origIdx]
	}
	// Add sentinel value for easier range calculation
	buf.fieldTIDs = append(buf.fieldTIDs, uint32(len(tokens)))

	// Store in index
	idx.tokens = sortedTokens
	idx.tokenLIDs = sortedTokenLIDs

	// Organize fields
	fieldBuffer := make([]byte, 0, fieldSize)
	idx.fields = make([][]byte, len(buf.fields))
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
	buffer := res.Bytes().AllocSlice(int(block.RawLen()))
	payload, err := block.DecompressTo(buffer)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// decodeMetadata decodes document metadata from binary format
// Format: [size: uint32][data: size bytes][size: uint32][data: size bytes]...
func decodeMetadata(res *Resources, buf *indexBuffer, payload []byte, sw *stopwatch.Stopwatch) ([]indexer.MetaData, error) {
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
	meta := res.Metadata().AllocSlice(len(buf.sizes))
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
