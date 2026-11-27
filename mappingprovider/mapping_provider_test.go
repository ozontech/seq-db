package mappingprovider

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/seq"
)

const initialMappingData = `mapping-list:
  - name: "k8s_pod"
    type: "keyword"
`

const changedMappingData = `mapping-list:
  - name: "k8s_pod"
    type: "keyword"
  - name: "trace_id"
    type: "keyword"
`

func TestMappingProvider(t *testing.T) {
	dir := t.TempDir()
	mappingFilePath := filepath.Join(dir, "mappings.yaml")

	err := os.WriteFile(mappingFilePath, []byte(initialMappingData), 0o644)
	require.NoError(t, err)

	mappingProvider, err := New(mappingFilePath, WithUpdatePeriod(100*time.Millisecond))
	require.NoError(t, err)

	assert.Equal(t, seq.Mapping{"k8s_pod": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0)}, mappingProvider.GetMapping())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mappingProvider.WatchUpdates(ctx)

	err = os.WriteFile(mappingFilePath, []byte(changedMappingData), 0o644)
	require.NoError(t, err)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, seq.Mapping{
			"k8s_pod":  seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
			"trace_id": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		}, mappingProvider.GetMapping())
	}, 5*time.Second, 500*time.Millisecond)
}

func TestMappingProviderSortingKeywordField(t *testing.T) {
	keywordMapping := seq.Mapping{"k8s_pod": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0)}
	for _, tc := range []struct {
		name      string
		mapping   seq.Mapping
		field     string
		indexAll  bool
		expectErr bool
	}{
		{name: "keyword", mapping: keywordMapping, field: "k8s_pod"},
		{name: "missing", mapping: keywordMapping, field: "service", expectErr: true},
		{name: "text", mapping: seq.Mapping{"k8s_pod": seq.NewSingleType(seq.TokenizerTypeText, "", 0)}, field: "k8s_pod", expectErr: true},
		{name: "path", mapping: seq.Mapping{"k8s_pod": seq.NewSingleType(seq.TokenizerTypePath, "", 0)}, field: "k8s_pod", expectErr: true},
		{name: "sorting disabled", mapping: keywordMapping},
		{name: "index all", field: "service", indexAll: true},
		{name: "index all with mapping", mapping: keywordMapping, field: "service", indexAll: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New("", WithMapping(tc.mapping), WithSortingKeywordField(tc.field), WithIndexAllFields(tc.indexAll))
			if tc.expectErr {
				require.ErrorContains(t, err, "must exist and have type keyword")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestMappingProviderRejectKeywordChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mapping.yaml")
	require.NoError(t, os.WriteFile(path, []byte(initialMappingData), 0o600))
	p, err := New(path, WithSortingKeywordField("k8s_pod"))
	require.NoError(t, err)
	original := p.GetMapping()
	raw := p.GetRawMapping().GetRawMappingBytes()
	checksum := p.checksum

	for _, tc := range []struct {
		name string
		data string
	}{
		{name: "deleted", data: strings.ReplaceAll(initialMappingData, "k8s_pod", "service")},
		{name: "changed type", data: strings.ReplaceAll(initialMappingData, "keyword", "text")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, os.WriteFile(path, []byte(tc.data), 0o600))
			_, err := New(path, WithSortingKeywordField("k8s_pod"))
			require.ErrorContains(t, err, "must exist and have type keyword")

			p.reloadMapping()
			require.Equal(t, original, p.GetMapping())
			require.Equal(t, raw, p.GetRawMapping().GetRawMappingBytes())
			require.Equal(t, checksum, p.checksum)
		})
	}

	require.NoError(t, os.WriteFile(path, []byte(changedMappingData), 0o600))
	p.reloadMapping()
	expected, err := seq.ReadMapping([]byte(changedMappingData))
	require.NoError(t, err)
	require.Equal(t, expected, p.GetMapping())
	require.Equal(t, seq.NewRawMapping(expected).GetRawMappingBytes(), p.GetRawMapping().GetRawMappingBytes())
	require.Equal(t, p.calcChecksum([]byte(changedMappingData)), p.checksum)
}
