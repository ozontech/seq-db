package processor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/metric/stopwatch"
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

func (d *testTokenIndex) GetTIDsByField(field string) ([]uint32, error) {
	panic("not implemented")
}

func (d *testTokenIndex) GetLIDsFromTIDs(tids []uint32, stats lids.Counter, minLID, maxLID uint32, order seq.DocsOrder) []node.Node {
	panic("not implemented")
}

func (d *testTokenIndex) GetBatchedLIDsFromTIDs(tids []uint32, stats lids.Counter, minLID, maxLID uint32, order seq.DocsOrder) []node.BatchedNode {
	nodes := make([]node.BatchedNode, len(tids))
	for i := range tids {
		nodes[i] = node.EmptyBatched()
	}
	return nodes
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

func TestQueryIterationCost(t *testing.T) {
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
		cost, err := calculateQueryIterationCost(q.Root, index, make(leafTIDsCache))
		require.NoError(t, err)
		assert.Equal(t, uint64(80_000), cost)
	})

	t.Run("and", func(t *testing.T) {
		q, err := parser.ParseSeqQL(`a:1 AND b:2`, nil)
		require.NoError(t, err)
		cost, err := calculateQueryIterationCost(q.Root, index, make(leafTIDsCache))
		require.NoError(t, err)
		assert.Equal(t, uint64(80_000), cost)
	})

	t.Run("or", func(t *testing.T) {
		q, err := parser.ParseSeqQL(`a:1 OR b:2`, nil)
		require.NoError(t, err)
		cost, err := calculateQueryIterationCost(q.Root, index, make(leafTIDsCache))
		require.NoError(t, err)
		assert.Equal(t, uint64(200_000), cost)
	})

	t.Run("and not", func(t *testing.T) {
		q, err := parser.ParseSeqQL(`a:1 AND NOT b:2`, nil)
		require.NoError(t, err)
		cost, err := calculateQueryIterationCost(q.Root, index, make(leafTIDsCache))
		require.NoError(t, err)
		assert.Equal(t, uint64(200_000), cost)
	})

	t.Run("nested and-or", func(t *testing.T) {
		q, err := parser.ParseSeqQL(`a:1 AND (b:2 OR c:3)`, nil)
		require.NoError(t, err)
		cost, err := calculateQueryIterationCost(q.Root, index, make(leafTIDsCache))
		require.NoError(t, err)
		assert.Equal(t, uint64(80_000), cost)
	})
}

func TestTryBuildBatchEvalTree(t *testing.T) {
	const threshold = 50_000
	queryOpt := QueryOptimizationConfig{BatchExecution: BatchExecutionConfig{Enabled: true, CostThreshold: threshold}}
	sw := stopwatch.New()

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
		stats := &searchStats{}
		tree, err := tryBuildBatchEvalTree(q.Root, config.BinaryDataV6, denseIndex, queryOpt, 1, 100, stats, seq.DocsOrderDesc, sw)
		require.NoError(t, err)
		assert.NotNil(t, tree)
		assert.Equal(t, 2, stats.LeavesTotal)
		assert.Equal(t, 3, stats.NodesTotal) // 2 leaves + 1 AND
	})

	t.Run("disabled skips batching", func(t *testing.T) {
		disabled := QueryOptimizationConfig{BatchExecution: BatchExecutionConfig{Enabled: false, CostThreshold: threshold}}
		q, err := parser.ParseSeqQL(`a:1 AND b:2`, nil)
		require.NoError(t, err)
		stats := &searchStats{}
		tree, err := tryBuildBatchEvalTree(q.Root, config.BinaryDataV6, denseIndex, disabled, 1, 100, stats, seq.DocsOrderDesc, sw)
		require.ErrorIs(t, err, errBatchingUnsupported)
		assert.Nil(t, tree)
		assert.Equal(t, 0, stats.LeavesTotal)
		assert.Equal(t, 0, stats.NodesTotal)
	})

	t.Run("sparse and query disables batching", func(t *testing.T) {
		q, err := parser.ParseSeqQL(`a:1 AND b:2`, nil)
		require.NoError(t, err)
		stats := &searchStats{}
		tree, err := tryBuildBatchEvalTree(q.Root, config.BinaryDataV6, sparseIndex, queryOpt, 1, 100, stats, seq.DocsOrderDesc, sw)
		require.ErrorIs(t, err, errBatchingUnsupported)
		assert.Nil(t, tree)
		assert.Equal(t, 0, stats.LeavesTotal)
		assert.Equal(t, 0, stats.NodesTotal)
	})

	t.Run("exactly at threshold disables batching", func(t *testing.T) {
		index := &testTokenIndex{
			tids:  map[string][]uint32{"a:1": {1}},
			freqs: map[uint32]uint32{1: threshold},
		}
		q, err := parser.ParseSeqQL(`a:1`, nil)
		require.NoError(t, err)
		stats := &searchStats{}
		tree, err := tryBuildBatchEvalTree(q.Root, config.BinaryDataV6, index, queryOpt, 1, 100, stats, seq.DocsOrderDesc, sw)
		require.ErrorIs(t, err, errBatchingUnsupported)
		assert.Nil(t, tree)
		assert.Equal(t, 0, stats.LeavesTotal)
		assert.Equal(t, 0, stats.NodesTotal)
	})

	t.Run("too many tids disables batching without mutating stats", func(t *testing.T) {
		index := &testTokenIndex{
			tids: map[string][]uint32{
				"a:1": {1},
				"b:2": {10, 11, 12, 13, 14, 15},
			},
			freqs: map[uint32]uint32{
				1:  120_000,
				10: 20_000,
				11: 20_000,
				12: 20_000,
				13: 20_000,
				14: 20_000,
				15: 20_000,
			},
		}
		q, err := parser.ParseSeqQL(`a:1 AND b:2`, nil)
		require.NoError(t, err)
		stats := &searchStats{}
		tree, err := tryBuildBatchEvalTree(q.Root, config.BinaryDataV6, index, queryOpt, 1, 100, stats, seq.DocsOrderDesc, sw)
		require.ErrorIs(t, err, errBatchingUnsupported)
		assert.Nil(t, tree)
		assert.Equal(t, 0, stats.LeavesTotal)
		assert.Equal(t, 0, stats.NodesTotal)
	})

	t.Run("not query disables batching without mutating stats", func(t *testing.T) {
		q, err := parser.ParseSeqQL(`NOT a:1`, nil)
		require.NoError(t, err)
		stats := &searchStats{}
		tree, err := tryBuildBatchEvalTree(q.Root, config.BinaryDataV6, denseIndex, queryOpt, 1, 100, stats, seq.DocsOrderDesc, sw)
		require.ErrorIs(t, err, errBatchingUnsupported)
		assert.Nil(t, tree)
		assert.Equal(t, 0, stats.LeavesTotal)
		assert.Equal(t, 0, stats.NodesTotal)
	})
}
