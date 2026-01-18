package indexer

import (
	"encoding/binary"
	"strings"

	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/tokenizer"
	"github.com/ozontech/seq-db/util"
)

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

func (dp *TestDocProvider) appendMeta(docLen int, id seq.ID, tokens []tokenizer.MetaToken) {
	dp.buf = dp.buf[:4]
	dp.buf = encodeMeta(dp.buf, tokens, id, docLen)
	binary.LittleEndian.PutUint32(dp.buf, uint32(len(dp.buf)-4))

	dp.Metas = append(dp.Metas, dp.buf...)
}

func (dp *TestDocProvider) Append(doc []byte, id seq.ID, tokensStr ...string) {
	tokens := stringsToTokens(tokensStr...)
	dp.appendMeta(len(doc), id, tokens)
	dp.appendDoc(doc)
}

func (dp *TestDocProvider) TryReset() {
	dp.DocCount = 0
	dp.Docs = dp.Docs[:0]
	dp.Metas = dp.Metas[:0]

}

func (dp *TestDocProvider) Provide() (storage.DocBlock, storage.DocBlock) {
	c := GetDocsMetasCompressor(-1, -1)
	c.CompressDocsAndMetas(dp.Docs, dp.Metas)
	return c.DocsMetas()
}

func encodeMeta(buf []byte, tokens []tokenizer.MetaToken, id seq.ID, size int) []byte {
	metaTokens := make([]tokenizer.MetaToken, 0, len(tokens))
	for _, t := range tokens {
		metaTokens = append(metaTokens, tokenizer.MetaToken{
			Key:   t.Key,
			Value: t.Value,
		})
	}
	md := MetaData{
		ID:     id,
		Size:   uint32(size),
		Tokens: metaTokens,
	}
	return md.MarshalBinaryTo(buf)
}

func stringsToTokens(tokens ...string) []tokenizer.MetaToken {
	r := make([]tokenizer.MetaToken, 0)
	for _, tokenStr := range tokens {
		fieldPos := strings.IndexByte(tokenStr, ':')
		var t tokenizer.MetaToken
		if fieldPos < 0 {
			t = tokenizer.MetaToken{
				Key:   util.StringToByteUnsafe(tokenStr),
				Value: []byte("some_val")}
		} else {
			t = tokenizer.MetaToken{
				Key:   util.StringToByteUnsafe(tokenStr[:fieldPos]),
				Value: util.StringToByteUnsafe(tokenStr[fieldPos+1:]),
			}
		}
		r = append(r, t)
	}
	return r
}
