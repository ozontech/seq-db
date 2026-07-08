package processor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
)

func TestASTSupportsBatching(t *testing.T) {
	t.Run("single field", func(t *testing.T) {
		q, err := parser.ParseSeqQL(`service:"foo"`, nil)
		require.NoError(t, err)
		assert.True(t, astSupportsBatching(q.Root))
	})

	t.Run("and of fields", func(t *testing.T) {
		q, err := parser.ParseSeqQL(`service:"foo" AND level:"error"`, nil)
		require.NoError(t, err)
		assert.True(t, astSupportsBatching(q.Root))
	})

	t.Run("nested and", func(t *testing.T) {
		q, err := parser.ParseSeqQL(`a:1 AND (b:2 AND c:3)`, nil)
		require.NoError(t, err)
		assert.True(t, astSupportsBatching(q.Root))
	})

	t.Run("or", func(t *testing.T) {
		q, err := parser.ParseSeqQL(`a:1 OR b:2`, nil)
		require.NoError(t, err)
		assert.True(t, astSupportsBatching(q.Root))
	})

	t.Run("not", func(t *testing.T) {
		q, err := parser.ParseSeqQL(`NOT a:1`, nil)
		require.NoError(t, err)
		assert.False(t, astSupportsBatching(q.Root))
	})

	t.Run("and with or child", func(t *testing.T) {
		q, err := parser.ParseSeqQL(`a:1 AND (b:2 OR c:3)`, nil)
		require.NoError(t, err)
		assert.True(t, astSupportsBatching(q.Root))
	})
}

type testTokenIndex struct {
	tids  map[string][]uint32
	freqs map[uint32]uint32
}

func (d *testTokenIndex) GetValByTID(tid uint32, _ string) []byte {
	panic("not implemented")
}

func (d *testTokenIndex) GetLIDsFromTIDs(tids []uint32, stats lids.Counter, minLID, maxLID uint32, order seq.DocsOrder) []node.Node {
	panic("not implemented")
}

func (d *testTokenIndex) GetBatchedLIDsFromTIDs(tids []uint32, stats lids.Counter, minLID, maxLID uint32, order seq.DocsOrder) []node.BatchedNode {
	panic("not implemented")
}

func (d *testTokenIndex) GetTIDsByTokenExpr(token parser.Token) ([]uint32, error) {
	key := parser.GetField(token) + ":" + parser.GetHint(token)
	return d.tids[key], nil
}

func (d *testTokenIndex) GetFreqsByTIDs(tids []uint32, _ string) []uint32 {
	freqs := make([]uint32, len(tids))
	for i, tid := range tids {
		freqs[i] = d.freqs[tid]
	}
	return freqs
}

func TestEstimateQueryDensity(t *testing.T) {
	index := &testTokenIndex{
		tids: map[string][]uint32{
			"a:1": {1},
			"b:2": {2},
			"c:3": {3},
		},
		freqs: map[uint32]uint32{
			1: 80_000,
			2: 120_000,
			3: 40_000,
		},
	}

	t.Run("leaf", func(t *testing.T) {
		q, err := parser.ParseSeqQL(`a:1`, nil)
		require.NoError(t, err)
		density, err := queryIterationCost(q.Root, index)
		require.NoError(t, err)
		assert.Equal(t, uint64(80_000), density)
	})

	t.Run("and uses min", func(t *testing.T) {
		q, err := parser.ParseSeqQL(`a:1 AND b:2`, nil)
		require.NoError(t, err)
		density, err := queryIterationCost(q.Root, index)
		require.NoError(t, err)
		assert.Equal(t, uint64(80_000), density)
	})

	t.Run("or uses sum", func(t *testing.T) {
		q, err := parser.ParseSeqQL(`a:1 OR b:2`, nil)
		require.NoError(t, err)
		density, err := queryIterationCost(q.Root, index)
		require.NoError(t, err)
		assert.Equal(t, uint64(200_000), density)
	})

	t.Run("nested and-or", func(t *testing.T) {
		q, err := parser.ParseSeqQL(`a:1 AND (b:2 OR c:3)`, nil)
		require.NoError(t, err)
		density, err := queryIterationCost(q.Root, index)
		require.NoError(t, err)
		assert.Equal(t, uint64(80_000), density) // max(80k, 120k+40k)
	})
}

func TestCanEnableBatching(t *testing.T) {
	const threshold = 50_000
	queryOpt := QueryOptimizationConfig{BatchIterationCostThreshold: threshold}

	denseIndex := &testTokenIndex{
		tids: map[string][]uint32{
			"a:1": {1},
			"b:2": {2},
		},
		freqs: map[uint32]uint32{
			1: 120_000,
			2: 120_000,
		},
	}
	sparseIndex := &testTokenIndex{
		tids: map[string][]uint32{
			"a:1": {1},
			"b:2": {2},
		},
		freqs: map[uint32]uint32{
			1: 1_000,
			2: 2_000,
		},
	}

	t.Run("dense and query enables batching", func(t *testing.T) {
		q, err := parser.ParseSeqQL(`a:1 AND b:2`, nil)
		require.NoError(t, err)
		assert.True(t, canEnableBatching(q.Root, denseIndex, queryOpt))
	})

	t.Run("sparse and query disables batching", func(t *testing.T) {
		q, err := parser.ParseSeqQL(`a:1 AND b:2`, nil)
		require.NoError(t, err)
		assert.False(t, canEnableBatching(q.Root, sparseIndex, queryOpt))
	})

	t.Run("exactly at threshold disables batching", func(t *testing.T) {
		index := &testTokenIndex{
			tids:  map[string][]uint32{"a:1": {1}},
			freqs: map[uint32]uint32{1: threshold},
		}
		q, err := parser.ParseSeqQL(`a:1`, nil)
		require.NoError(t, err)
		assert.False(t, canEnableBatching(q.Root, index, queryOpt))
	})
}
