package processor

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
)

type staticProvider struct {
	data           map[string][]uint32
	ignoreNotFound bool
}

func newProvider(ignore bool) *staticProvider {
	return &staticProvider{
		data:           map[string][]uint32{},
		ignoreNotFound: ignore,
	}
}

func (p *staticProvider) add(token string, data []uint32) {
	p.data[token] = data
}

func (p *staticProvider) provide(token string) ([]uint32, error) {
	data, has := p.data[token]
	if !has {
		if p.ignoreNotFound {
			return []uint32{}, nil
		}
		return nil, fmt.Errorf("token %q not found", token)
	}
	return data, nil
}

func (p *staticProvider) newStatic(literal *parser.Literal) (node.Node, error) {
	builder := &strings.Builder{}
	literal.DumpSeqQL(builder)
	data, err := p.provide(builder.String())
	if err != nil {
		return nil, err
	}
	return node.NewStatic(data, true), nil
}

func readAllInto(n node.Node, ids []uint32) []uint32 {
	for cmp := n.Next(); !cmp.IsNull(); cmp = n.Next() {
		ids = append(ids, cmp.Unpack())
	}
	return ids
}

func readAll(n node.Node) []uint32 {
	return readAllInto(n, nil)
}

func TestEval(t *testing.T) {
	p := newProvider(false)
	p.add("m:a", []uint32{1, 2, 7, 8})
	p.add("m:b", []uint32{3, 6, 7, 8, 9, 10, 13})
	// in this case NAND = [3, 6, 9, 10, 13]
	p.add("m:c", []uint32{1, 3, 4, 5, 6, 9})
	p.add("m:d", []uint32{3, 4, 10, 12, 15})
	// in this case OR = [1, 3, 4, 5, 6, 9, 10, 12, 15]
	// and AND = [3, 6, 9, 10]
	newStatic := func(l parser.Token) (node.Node, error) {
		return p.newStatic(l.(*parser.Literal))
	}

	t.Run("simple", func(t *testing.T) {
		query, err := parser.ParseSeqQL(`((NOT m:a AND m:b) AND (m:c OR m:d))`, nil)
		require.NoError(t, err)
		root, err := buildEvalTree(query.Root, 1, 12, &searchStats{}, false, newStatic)
		require.NoError(t, err)

		assert.Equal(t, "((STATIC NAND STATIC) AND (STATIC OR STATIC))", root.String())
		assert.Equal(t, []uint32{3, 6, 9, 10}, readAll(root))
	})

	t.Run("not", func(t *testing.T) {
		query, err := parser.ParseSeqQL(`NOT ((NOT m:a AND m:b) AND (m:c OR m:d))`, nil)
		require.NoError(t, err)
		root, err := buildEvalTree(query.Root, 1, 12, &searchStats{}, false, newStatic)
		require.NoError(t, err)

		assert.Equal(t, "(NOT ((STATIC NAND STATIC) AND (STATIC OR STATIC)))", root.String())
		assert.Equal(t, []uint32{1, 2, 4, 5, 7, 8, 11, 12}, readAll(root))
	})
}

func TestMatchAllNode(t *testing.T) {
	all := func(minLID, maxLID uint32, asc bool) []uint32 {
		return readAll(matchAllNode(minLID, maxLID, asc))
	}

	// In node package convention asc means LID numbers ascend, which is
	// desc document order: LID 1 is the newest document.
	t.Run("asc lids", func(t *testing.T) {
		assert.Equal(t, []uint32{3, 4, 5}, all(3, 5, true))
	})
	t.Run("desc lids", func(t *testing.T) {
		assert.Equal(t, []uint32{5, 4, 3}, all(3, 5, false))
	})
	t.Run("single lid window", func(t *testing.T) {
		assert.Equal(t, []uint32{2}, all(2, 2, false))
	})
	t.Run("degenerate window", func(t *testing.T) {
		// maxLID = minLID-1 happens when the query range misses the fraction.
		assert.Empty(t, all(4, 3, false))
	})
}

type recordingProvider struct {
	tokenIndex
	calls []string
}

func (p *recordingProvider) GetTIDsByTokenExpr(token parser.Token) ([]uint32, error) {
	p.calls = append(p.calls, "GetTIDsByTokenExpr")
	return nil, nil
}

func (p *recordingProvider) GetTIDsByField(field string) ([]uint32, error) {
	p.calls = append(p.calls, "GetTIDsByField")
	return nil, nil
}

func (p *recordingProvider) GetLIDsFromTIDs(tids []uint32, stats lids.Counter, minLID, maxLID uint32, order seq.DocsOrder) []node.Node {
	p.calls = append(p.calls, "GetLIDsFromTIDs")
	return nil
}

func TestMatchAllDoesNotTouchIndex(t *testing.T) {
	p := &recordingProvider{}

	token := &parser.Literal{
		Field: seq.TokenAll,
		Terms: []parser.Term{{Kind: parser.TermSymbol, Data: "*"}},
	}
	leaf, err := evalLeaf(p, token, stopwatch.New(), &searchStats{}, 2, 7, seq.DocsOrderDesc)
	require.NoError(t, err)
	assert.Equal(t, []uint32{2, 3, 4, 5, 6, 7}, readAll(leaf))

	it, err := iteratorFromLiteral(
		p, token, stopwatch.New(), &searchStats{}, 2, 7,
		0, iteratorLimit{}, seq.DocsOrderDesc, QueryStats{}, QueryOptimizationConfig{},
	)
	require.NoError(t, err)
	assert.Equal(t, []uint32{0}, it.tids)

	assert.Empty(t, p.calls, "match-all filter must not query the index")
}

func TestIteratorFromLiteralMatchAllAggregation(t *testing.T) {
	token := &parser.Literal{
		Field: seq.TokenAll,
		Terms: []parser.Term{{Kind: parser.TermSymbol, Data: "*"}},
	}

	for _, order := range []seq.DocsOrder{seq.DocsOrderDesc, seq.DocsOrderAsc} {
		it, err := iteratorFromLiteral(
			&MockTokenIndex{}, token, stopwatch.New(), &searchStats{}, 1, 4,
			0, iteratorLimit{}, order, QueryStats{}, QueryOptimizationConfig{},
		)
		require.NoError(t, err)

		agg := NewSingleSourceCountAggregator(it, provideExtractTimeFunc(nil, nil, 0), false)
		for lid := uint32(1); lid <= 4; lid++ {
			docLID := lid
			if order.IsReverse() {
				docLID = 5 - lid
			}
			require.NoError(t, agg.Next([]node.LID{node.NewLID(docLID, order.IsDesc())}))
		}

		result, err := agg.Aggregate()
		require.NoError(t, err)

		assert.Len(t, result.SamplesByBin, 1)
		for bin, samples := range result.SamplesByBin {
			assert.Equal(t, "0", bin.Token)
			assert.Equal(t, int64(4), samples.Total)
		}
	}
}
