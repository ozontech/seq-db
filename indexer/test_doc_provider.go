package indexer

import (
	"encoding/binary"
	"math/rand"
	"strings"
	"time"

	insaneJSON "github.com/ozontech/insane-json"

	"github.com/ozontech/seq-db/consts"
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

func (dp *TestDocProvider) Append(doc []byte, docRoot *insaneJSON.Root, id seq.ID, tokensStr ...string) {
	tokens := stringsToTokens(tokensStr...)
	if id.MID == 0 {
		// this case runs only in the integration tests
		t, _ := extractDocTimeForTest(docRoot)
		id = seq.NewID(t, uint64(rand.Int63()))
	}

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

// extractDocTimeForTest extracts timestamp from doc
// It searches by one of supported field name and parses by supported formats
// If no field was found or not parsable it returns time.Now()
func extractDocTimeForTest(docRoot *insaneJSON.Root) (time.Time, []string) {
	var t time.Time
	var f []string
top:
	for _, field := range consts.TimeFields {
		timeNode := docRoot.Dig(field...)
		if timeNode == nil {
			continue
		}
		timeVal := timeNode.AsString()
		for _, format := range consts.TimeFormats {
			if value, err := time.Parse(format, timeVal); err == nil {
				t = value
				f = field
				break top
			}
		}
	}

	if t.IsZero() {
		t = time.Now()
	}
	return t, f
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
