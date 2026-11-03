package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/common"
)

// sealAndDecode seals docs (one JSON document per element, empty elements
// are skipped just like on stdin) into a fresh fraction in a temp dir
// using the mapping at mappingPath (empty string means the built-in
// default) and decodes the requested sections back into a collected form
// for assertions.
func sealAndDecode(
	t *testing.T,
	mappingPath string,
	docsPerDocBlock int,
	only string,
	docs ...string,
) *collectedContent {
	t.Helper()

	fracName := filepath.Join(t.TempDir(), "frac_000001")
	r := strings.NewReader(strings.Join(docs, "\n"))

	err := sealFraction(fracName, mappingPath, r, docsPerDocBlock)
	require.NoError(t, err)

	for _, suffix := range []string{
		consts.InfoFileSuffix, consts.DocsFileSuffix, consts.TokenFileSuffix,
		consts.OffsetsFileSuffix, consts.IDFileSuffix, consts.LIDFileSuffix,
	} {
		_, err := os.Stat(fracName + suffix)
		require.NoError(t, err, "missing %s file", suffix)
	}

	content := &collectedContent{t: t}
	err = decodeFraction(fracName, only, content)
	require.NoError(t, err)

	return content
}

type collectedContent struct {
	t *testing.T

	Info    *infoRecord
	Docs    []docRecord
	IDs     []idRecord
	Tokens  []tokenRecord
	Offsets *offsetsRecord
}

func (c *collectedContent) Write(p []byte) (int, error) {
	for _, line := range strings.Split(string(p), "\n") {
		if line == "" {
			continue
		}

		var probe struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal([]byte(line), &probe); err != nil {
			return 0, err
		}

		switch probe.Kind {
		case kindInfo:
			require.Nil(c.t, c.Info, "duplicate info record")
			var info common.Info
			err := json.Unmarshal([]byte(line), &info)
			c.check(err)
			c.Info = &infoRecord{Kind: probe.Kind, Info: &info}
		case kindDoc:
			var rec docRecord
			err := json.Unmarshal([]byte(line), &rec)
			c.check(err)
			c.Docs = append(c.Docs, rec)
		case kindID:
			var rec idRecord
			err := json.Unmarshal([]byte(line), &rec)
			c.check(err)
			c.IDs = append(c.IDs, rec)
		case kindToken:
			var rec tokenRecord
			err := json.Unmarshal([]byte(line), &rec)
			c.check(err)
			c.Tokens = append(c.Tokens, rec)
		case kindOffsets:
			require.Nil(c.t, c.Offsets, "duplicate offsets record")
			var rec offsetsRecord
			err := json.Unmarshal([]byte(line), &rec)
			c.check(err)
			c.Offsets = &rec
		default:
			c.t.Fatalf("unknown record kind: %q", probe.Kind)
		}
	}

	return len(p), nil
}

func (c *collectedContent) check(err error) {
	require.NoError(c.t, err)
}

const testDocs = `
{"timestamp":"2024-01-01T10:00:00.000Z","level":"info","service":"auth","message":"user logged in"}
{"timestamp":"2024-01-01T10:00:01.000Z","level":"error","service":"auth","message":"token expired"}
{"timestamp":"2024-01-01T10:00:02.000Z","level":"info","service":"billing","message":"payment ok"}
`

var testTokens = []tokenRecord{
	{Field: "_all_", Token: "", Freq: 3},
	{Field: "_exists_", Token: "level", Freq: 3},
	{Field: "_exists_", Token: "message", Freq: 3},
	{Field: "_exists_", Token: "service", Freq: 3},
	{Field: "level", Token: "error", Freq: 1},
	{Field: "level", Token: "info", Freq: 2},
	{Field: "message", Token: "expired", Freq: 1},
	{Field: "message", Token: "in", Freq: 1},
	{Field: "message", Token: "logged", Freq: 1},
	{Field: "message", Token: "ok", Freq: 1},
	{Field: "message", Token: "payment", Freq: 1},
	{Field: "message", Token: "token", Freq: 1},
	{Field: "message", Token: "user", Freq: 1},
	{Field: "service", Token: "auth", Freq: 2},
	{Field: "service", Token: "billing", Freq: 1},
}

func TestSealSkipsEmptyLines(t *testing.T) {
	content := sealAndDecode(t, "", defaultDocsPerDocBlock, kindInfo+","+kindDoc,
		`{"timestamp":"2024-01-01T10:00:00.000Z","level":"info","message":"first"}`,
		"",
		`{"timestamp":"2024-01-01T10:00:01.000Z","level":"info","message":"second"}`,
	)

	require.NotNil(t, content.Info)
	assert.Equal(t, uint32(2), content.Info.DocsTotal)
	assert.Len(t, content.Docs, 2)
}

func TestSealSplitsIntoDocBlocks(t *testing.T) {
	content := sealAndDecode(t, "", 2, kindInfo+","+kindDoc+","+kindID+","+kindToken+","+kindOffsets,
		generateDocs(5)...,
	)

	// 5 docs with 2 docs per block: [2][2][1]
	require.Len(t, content.Offsets.Values, 3)
	assert.Equal(t, uint32(5), content.Info.DocsTotal)

	// all docs are decoded, ids have contiguous lids across block boundaries
	require.Len(t, content.Docs, 5)
	require.Len(t, content.IDs, 6) // +1 for the system id
	for i, id := range content.IDs {
		assert.Equal(t, uint32(i), id.LID)
	}

	// every posting lid points to an existing doc
	for _, tok := range content.Tokens {
		assert.Len(t, tok.LIDs, int(tok.Freq))
		for _, lid := range tok.LIDs {
			assert.NotZero(t, lid)
			assert.LessOrEqual(t, lid, uint32(5))
		}
	}

	// every doc position points into a real doc block, and every block is
	// used: doc indexers assign block indexes concurrently, so only the
	// counts per block are guaranteed, not the order
	blocksPerLID := map[uint32]int{}
	for _, id := range content.IDs[1:] { // system id is not positioned
		assert.Less(t, id.BlockIndex, uint32(len(content.Offsets.Values)), "block index out of range")
		blocksPerLID[id.BlockIndex]++
	}
	assert.Equal(t, map[uint32]int{0: 2, 1: 2, 2: 1}, blocksPerLID)
}

func TestSealFailures(t *testing.T) {
	tests := []struct {
		name string
		in   string
	}{
		{
			name: "empty input",
			in:   "",
		},
		{
			name: "invalid json",
			in:   `{not json`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fracName := filepath.Join(t.TempDir(), "frac_000001")

			err := sealFraction(fracName, "", strings.NewReader(tt.in), defaultDocsPerDocBlock)
			assert.Error(t, err)
		})
	}
}
func TestSealCustomMapping(t *testing.T) {
	mapping := filepath.Join(t.TempDir(), "mapping.yaml")
	err := os.WriteFile(mapping, []byte("mapping-list:\n  - type: \"path\"\n    name: \"request_uri\"\n"), 0o600)
	require.NoError(t, err)

	content := sealAndDecode(t, mapping, defaultDocsPerDocBlock, kindToken,
		`{"timestamp":"2024-01-01T10:00:00.000Z","request_uri":"/api/v1/users"}`,
	)

	require.Len(t, content.Tokens, 5)
	assert.Equal(t, "_all_", content.Tokens[0].Field)
	assert.Equal(t, "_exists_", content.Tokens[1].Field)
	assert.Equal(t, "request_uri", content.Tokens[1].Token)
	assert.Equal(t, "request_uri", content.Tokens[2].Field)
	assert.Equal(t, "/api", content.Tokens[2].Token)
	assert.Equal(t, "/api/v1", content.Tokens[3].Token)
	assert.Equal(t, "/api/v1/users", content.Tokens[4].Token)
}

func TestSealMissingMapping(t *testing.T) {
	fracName := filepath.Join(t.TempDir(), "frac_000001")

	err := sealFraction(fracName, filepath.Join(t.TempDir(), "no_such_mapping.yaml"), strings.NewReader(`{}`), defaultDocsPerDocBlock)
	assert.Error(t, err)
}

func TestSealInvalidMapping(t *testing.T) {
	mapping := filepath.Join(t.TempDir(), "mapping.yaml")
	err := os.WriteFile(mapping, []byte("not a mapping"), 0o600)
	require.NoError(t, err)

	fracName := filepath.Join(t.TempDir(), "frac_000001")

	err = sealFraction(fracName, mapping, strings.NewReader(`{}`), defaultDocsPerDocBlock)
	assert.Error(t, err)
}

func generateDocs(n int) []string {
	docs := make([]string, 0, n)
	for i := range n {
		docs = append(docs, fmt.Sprintf(
			`{"timestamp":"2024-01-01T10:00:00.000Z","level":"info","service":"auth","message":"doc %d"}`,
			i,
		))
	}
	return docs
}

func docLines(t *testing.T, docs string) []string {
	t.Helper()

	var lines []string
	for _, line := range strings.Split(strings.TrimSpace(docs), "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func docTexts(docs []docRecord) []string {
	texts := make([]string, len(docs))
	for i, doc := range docs {
		texts[i] = doc.Doc
	}
	return texts
}
