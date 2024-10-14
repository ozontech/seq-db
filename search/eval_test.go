package search

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
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
	data, err := p.provide(literal.String())
	if err != nil {
		return nil, err
	}
	return node.NewStatic(data, false), nil
}

func readAllInto(n node.Node, ids []uint32) []uint32 {
	id, has := n.Next()
	for has {
		ids = append(ids, id)
		id, has = n.Next()
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
	newStatic := func(l parser.Token, _ *Stats) (node.Node, error) {
		return p.newStatic(l.(*parser.Literal))
	}

	t.Run("simple", func(t *testing.T) {
		ast, err := parser.ParseQuery(`((NOT m:a AND m:b) AND (m:c OR m:d))`, nil)
		require.NoError(t, err)
		root, err := buildEvalTree(ast, 1, 12, NewStats(frac.TypeActive), false, newStatic)
		require.NoError(t, err)

		assert.Equal(t, "((STATIC NAND STATIC) AND (STATIC OR STATIC))", root.String())
		assert.Equal(t, []uint32{3, 6, 9, 10}, readAll(root))
	})

	t.Run("not", func(t *testing.T) {
		ast, err := parser.ParseQuery(`NOT ((NOT m:a AND m:b) AND (m:c OR m:d))`, nil)
		require.NoError(t, err)
		root, err := buildEvalTree(ast, 1, 12, NewStats(frac.TypeActive), false, newStatic)
		require.NoError(t, err)

		assert.Equal(t, "(NOT ((STATIC NAND STATIC) AND (STATIC OR STATIC)))", root.String())
		assert.Equal(t, []uint32{1, 2, 4, 5, 7, 8, 11, 12}, readAll(root))
	})
}
