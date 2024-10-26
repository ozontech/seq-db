package frac

import (
	"encoding/binary"
	"math/rand"
	"time"

	insaneJSON "github.com/ozontech/insane-json"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/seq"
)

type DocProvider struct {
	DocCount int
	Docs     []byte
	Metas    []byte
	buf      []byte
}

func NewDocProvider() *DocProvider {
	return &DocProvider{
		Docs: make([]byte, 0),
		buf:  make([]byte, 4),
	}
}

func (dp *DocProvider) appendDoc(doc []byte) {
	dp.DocCount++
	numBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(numBuf, uint32(len(doc)))
	dp.Docs = append(dp.Docs, numBuf...)
	dp.Docs = append(dp.Docs, doc...)
}

func (dp *DocProvider) appendMeta(docLen int, id seq.ID, tokens []seq.Token) {
	dp.buf = dp.buf[:4]
	dp.buf = encodeMeta(dp.buf, tokens, id, docLen)
	binary.LittleEndian.PutUint32(dp.buf, uint32(len(dp.buf)-4))

	dp.Metas = append(dp.Metas, dp.buf...)
}

func (dp *DocProvider) Append(doc []byte, docRoot *insaneJSON.Root, id seq.ID, tokens []seq.Token) {
	if id.MID == 0 {
		// this case runs only in the integration tests
		t, _ := ExtractDocTime(docRoot)
		id = seq.NewID(t, uint64(rand.Int63()))
	}

	dp.appendMeta(len(doc), id, tokens)
	dp.appendDoc(doc)
}

func (dp *DocProvider) TryReset() {
	dp.DocCount = 0
	dp.Docs = dp.Docs[:0]
	dp.Metas = dp.Metas[:0]

}

func (dp *DocProvider) Provide() (disk.DocBlock, disk.DocBlock) {
	c := GetDocsMetasCompressor(-1, -1)
	c.CompressDocsAndMetas(dp.Docs, dp.Metas)
	return c.DocsMetas()
}

func encodeMeta(buf []byte, tokens []seq.Token, id seq.ID, size int) []byte {
	metaTokens := make([]MetaToken, 0, len(tokens))
	for _, t := range tokens {
		metaTokens = append(metaTokens, MetaToken{
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

// extractDocTime extract time from doc by supported fields and return that field
// if fields are absent or values are not parsable, zero time and empty string are returned
func extractDocTime(docRoot *insaneJSON.Root) (time.Time, []string) {
	var t time.Time
	var err error
	for _, field := range consts.TimeFields {
		timeNode := docRoot.Dig(field...)
		if timeNode == nil {
			continue
		}

		timeVal := timeNode.AsString()
		for _, f := range consts.TimeFormats {
			t, err = time.Parse(f, timeVal)
			if err == nil {
				return t, field
			}
		}
	}

	return t, nil
}

// ExtractDocTime extracts timestamp from doc
// It searches by one of supported field name and parses by supported formats
// If no field was found or not parsable it returns time.Now()
func ExtractDocTime(docRoot *insaneJSON.Root) (time.Time, []string) {
	t, f := extractDocTime(docRoot)
	if t.IsZero() {
		t = time.Now()
	}
	return t, f
}
