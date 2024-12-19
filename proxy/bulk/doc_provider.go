package bulk

import (
	"encoding/binary"
	"math/rand"
	"time"

	insaneJSON "github.com/ozontech/insane-json"

	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tokenizer"
)

// TestDocProvider for test
type TestDocProvider struct {
	DocCount int
	Docs     []byte
	Metas    []byte
	buf      []byte
}

func NewTestDocProvider() *TestDocProvider {
	return &TestDocProvider{
		Docs: make([]byte, 0),
		buf:  make([]byte, 4),
	}
}

func (dp *TestDocProvider) appendDoc(doc []byte) {
	dp.DocCount++
	numBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(numBuf, uint32(len(doc)))
	dp.Docs = append(dp.Docs, numBuf...)
	dp.Docs = append(dp.Docs, doc...)
}

func (dp *TestDocProvider) appendMeta(docLen int, id seq.ID, tokens []seq.Token) {
	dp.buf = dp.buf[:4]
	dp.buf = encodeMeta(dp.buf, tokens, id, docLen)
	binary.LittleEndian.PutUint32(dp.buf, uint32(len(dp.buf)-4))

	dp.Metas = append(dp.Metas, dp.buf...)
}

func (dp *TestDocProvider) Append(doc []byte, docRoot *insaneJSON.Root, id seq.ID, tokens []seq.Token) {
	if id.MID == 0 {
		// this case runs only in the integration tests
		t, _ := extractDocTime(docRoot.Node, time.Now())
		id = seq.NewID(t, uint64(rand.Int63()))
	}

	dp.appendMeta(len(doc), id, tokens)
	dp.appendDoc(doc)
}

func (dp *TestDocProvider) Reset() {
	dp.DocCount = 0
	dp.Docs = dp.Docs[:0]
	dp.Metas = dp.Metas[:0]

}

func (dp *TestDocProvider) Provide() (disk.DocBlock, disk.DocBlock) {
	c := GetDocsMetasCompressor(-1, -1)
	c.CompressDocsAndMetas(dp.Docs, dp.Metas)
	return c.DocsMetas()
}

func encodeMeta(buf []byte, tokens []seq.Token, id seq.ID, size int) []byte {
	metaTokens := make([]tokenizer.MetaToken, 0, len(tokens))
	for _, t := range tokens {
		metaTokens = append(metaTokens, tokenizer.MetaToken{
			Key:   t.Field,
			Value: t.Val,
		})
	}
	md := MetaData{
		ID:     id,
		Size:   uint32(size),
		Tokens: metaTokens,
	}
	return md.MarshalBinaryTo(buf)
}
