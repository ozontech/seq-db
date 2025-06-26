package frac

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/seq"
	"github.com/stretchr/testify/require"
)

func TestDocBlocksWriter(t *testing.T) {
	r := require.New(t)

	payload := []byte(`"hello world"`)
	test := func(blockSize int, docsCount int, expectedBlocks int) {
		t.Helper()

		buf := bytes.NewBuffer(nil)

		bw := getDocBlocksWriter(buf, blockSize, 1)

		ids := make([]seq.ID, 0, docsCount)
		for range docsCount {
			id := nextSeqID()
			r.NoError(bw.WriteDoc(id, payload))
			ids = append(ids, id)
		}
		r.NoError(bw.Flush())

		r.Equal(expectedBlocks, len(bw.BlockOffsets))
		r.Equal(len(bw.Positions), len(ids))

		docBlocks := buf.Bytes()
		assertBlocks(t, docBlocks, expectedBlocks)

		for _, id := range ids {
			pos := bw.Positions[id]
			blockIndex, docOffset := pos.Unpack()
			blockOffset := bw.BlockOffsets[blockIndex]

			blockHeader := disk.DocBlock(docBlocks[blockOffset:])
			block := disk.DocBlock(docBlocks[blockOffset : blockOffset+blockHeader.FullLen()])

			binDocs, err := block.DecompressTo(nil)
			r.NoError(err)

			binDoc := binDocs[docOffset:]
			docLen := binary.LittleEndian.Uint32(binDoc)
			doc := binDoc[4 : 4+docLen]
			r.Equal(string(payload), string(doc))
		}
	}

	test(0, 4, 1)
	test(len(payload), 4, 4)
	test(len(payload)+2, 4, 4)
	test(len(payload)*2, 4, 2)
	test(len(payload)*4, 4, 1)
	test(len(payload)*4, 8, 2)
}

func assertBlocks(t *testing.T, docBlocks []byte, numBlocks int) {
	t.Helper()
	var n int
	for ; len(docBlocks) > 0; n++ {
		header := disk.DocBlock(docBlocks[:disk.DocBlockHeaderLen])
		docBlock := disk.DocBlock(docBlocks[:header.FullLen()])
		_, err := docBlock.DecompressTo(nil)
		require.NoError(t, err)
		docBlocks = docBlocks[docBlock.FullLen():]
	}
	require.Equal(t, numBlocks, n)
}

var (
	now     = time.Now()
	nextRid = 0
)

func nextSeqID() seq.ID {
	nextRid++
	return seq.ID{
		MID: seq.MID(now.Nanosecond()),
		RID: seq.RID(nextRid),
	}
}
