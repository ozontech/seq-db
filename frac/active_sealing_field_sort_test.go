package frac_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/compaction"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
)

func TestFieldSortedFractionTestSuite(t *testing.T) {
	suite.Run(t, &SealedFractionTestSuite{
		FractionTestSuite: FractionTestSuite{sortDocsField: "service"},
	})
}

func setupFieldSortTest(t *testing.T, field string) *FractionTestSuite {
	t.Helper()
	s := &FractionTestSuite{sortDocsField: field}
	s.SetT(t)
	s.SetupSuiteCommon()
	t.Cleanup(s.TearDownSuiteCommon)
	s.SetupTestCommon()
	t.Cleanup(s.TearDownTestCommon)
	return s
}

func TestSortDocsByField(t *testing.T) {
	aOld := `{"timestamp":"2000-01-01T00:00:01Z","service":"a"}`
	b := `{"timestamp":"2000-01-01T00:00:02Z","service":"b"}`
	aNew := `{"timestamp":"2000-01-01T00:00:03Z","service":"a"}`
	missing := `{"timestamp":"2000-01-01T00:00:04Z"}`
	empty := `{"timestamp":"2000-01-01T00:00:05Z","service":""}`
	spansZA := `{"timestamp":"2000-01-01T00:00:01Z","spans":[{"span_id":"z"},{"span_id":"a"}]}`
	spanB := `{"timestamp":"2000-01-01T00:00:02Z","spans":[{"span_id":"b"}]}`

	for _, tc := range []struct {
		name        string
		field       string
		docs        []string
		blocksByLID []uint32
	}{
		{
			name: "keyword then timestamp", field: "service",
			docs:        []string{aOld, b, aNew},
			blocksByLID: []uint32{1, 3, 2},
		},
		{
			name: "same timestamp sorted by RID", field: "service",
			docs:        []string{aOld, aOld},
			blocksByLID: []uint32{1, 2},
		},
		{
			name: "missing field last", field: "service",
			docs:        []string{missing, aOld},
			blocksByLID: []uint32{2, 1},
		},
		{
			name: "empty keyword first", field: "service",
			docs:        []string{aOld, empty},
			blocksByLID: []uint32{1, 2},
		},
		{
			name: "field absent in all documents", field: "status",
			docs:        []string{aOld, b},
			blocksByLID: []uint32{1, 2},
		},
		{
			name: "minimum keyword and shared body for nested LIDs", field: "spans.span_id",
			docs:        []string{spansZA, spanB},
			blocksByLID: []uint32{2, 2, 1, 1, 1},
		},
		{
			name: "nested LIDs without sort field", field: "service",
			docs:        []string{spansZA, spanB},
			blocksByLID: []uint32{1, 1, 2, 2, 2},
		},
		{
			name: "one document", field: "service",
			docs:        []string{aOld},
			blocksByLID: []uint32{1},
		},
		{name: "empty fraction", field: "service"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := setupFieldSortTest(t, tc.field)
			s.sealParams.DocBlockSize = 1
			active := s.newActive(tc.docs)
			defer active.Release()
			src, err := frac.NewActiveSealingSource(active, s.sealParams)
			require.NoError(t, err)
			var blocksByLID []uint32
			for loc, err := range src.IDs() {
				require.NoError(t, err)
				if loc.First == seq.SystemID {
					require.Equal(t, seq.SystemDocPos, loc.Second)
					continue
				}
				block, offset := loc.Second.Unpack()
				require.Zero(t, offset)
				blocksByLID = append(blocksByLID, block)
			}
			require.Equal(t, tc.blocksByLID, blocksByLID)
			require.Len(t, src.BlockOffsets(), len(tc.docs)+1)
		})
	}
}

func TestCompactionMixedDocsFormats(t *testing.T) {
	s := setupFieldSortTest(t, "")
	s.sealParams.DocBlockSize = 1
	var docs []string
	var sources []*frac.SealedSource
	for i, cfg := range []frac.Config{{SkipSortDocs: true}, {}, {SortDocsField: "service"}} {
		bulk := []string{
			fmt.Sprintf(`{"timestamp":"2000-01-01T00:00:%02dZ","service":"b"}`, i*2),
			fmt.Sprintf(`{"timestamp":"2000-01-01T00:00:%02dZ","service":"a"}`, i*2+1),
		}
		docs = append(docs, bulk...)
		s.config = &cfg
		sealed := s.newSealed(bulk)
		//nolint:gocritic
		defer sealed.Release()
		sources = append(sources, frac.NewSealedSource(sealed))
	}
	base := filepath.Join(s.tmpDir, "merged")
	_, err := compaction.Merge(base, s.sealParams, sources[0], sources[1], sources[2])
	require.NoError(t, err)
	merged := frac.NewSealed(base, storage.NewReadLimiter(1, nil), frac.NewIndexCache(), cache.NewConcurrentCache[[]byte](nil, nil), nil, &frac.Config{SkipSortDocs: true}, testSkipMaskProvider{})
	defer merged.Release()
	s.fraction = merged
	s.AssertSearch("", docs, []int{5, 4, 3, 2, 1, 0})
}

func TestFieldSortedDocsDeletion(t *testing.T) {
	for _, mode := range []frac.PSD{frac.Off, frac.HalfRename, frac.HalfRemove} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			s := setupFieldSortTest(t, "service")
			sealed := s.newSealed([]string{`{"timestamp":"2000-01-01T00:00:00Z","service":"a"}`})
			sealed.PartialSuicideMode = mode
			sealed.Suicide()
			require.NoFileExists(t, sealed.BaseFileName+consts.StdocsFileSuffix)
			if mode == frac.Off {
				require.NoFileExists(t, sealed.BaseFileName+consts.StdocsDelFileSuffix)
			} else {
				require.FileExists(t, sealed.BaseFileName+consts.StdocsDelFileSuffix)
			}
		})
	}
}
