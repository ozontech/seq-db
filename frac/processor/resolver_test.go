package processor

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/parser"
)

type recordingTokenIndex struct {
	testTokenIndex
	present map[string]bool
	lookups []string
}

func (d *recordingTokenIndex) GetTIDsByTokenExpr(token parser.Token) ([]uint32, error) {
	key := astDump(&parser.ASTNode{Value: token})
	d.lookups = append(d.lookups, key)
	if d.present[key] {
		return []uint32{1}, nil
	}
	return nil, nil
}

func TestASTResolver(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		present     []string
		wantEmpty   bool
		wantAST     string
		wantLookups []string
	}{
		{
			name:        "empty literal skips scan",
			query:       `a:x and b:*y*`,
			wantEmpty:   true,
			wantLookups: []string{`a:x`},
		},
		{
			name:        "literal goes before scan regardless of position",
			query:       `b:*y* and a:x`,
			present:     []string{`b:*y*`},
			wantEmpty:   true,
			wantLookups: []string{`a:x`},
		},
		{
			name:        "literal goes before prefix",
			query:       `s:wms* and r:1`,
			present:     []string{`s:wms*`},
			wantEmpty:   true,
			wantLookups: []string{`r:1`},
		},
		{
			name:        "nested and chain is flattened",
			query:       `((s:wms* and r:1) and t:1)`,
			present:     []string{`s:wms*`, `r:1`},
			wantEmpty:   true,
			wantLookups: []string{`r:1`, `t:1`},
		},
		{
			name:        "or of literals goes after single literals",
			query:       `(a:1 or b:2) and c:3`,
			wantEmpty:   true,
			wantLookups: []string{`c:3`},
		},
		{
			name:        "empty or branch is dropped",
			query:       `(a:1 or b:2) and c:3`,
			present:     []string{`a:1`, `c:3`},
			wantAST:     `(a:1 and c:3)`,
			wantLookups: []string{`c:3`, `a:1`, `b:2`},
		},
		{
			name:        "or of literals goes before scan",
			query:       `a:*x* and (b:1 or c:2)`,
			present:     []string{`a:*x*`},
			wantEmpty:   true,
			wantLookups: []string{`b:1`, `c:2`},
		},
		{
			name:        "empty and under not is pruned to its empty conjunct",
			query:       `not (b:*x* and c:2)`,
			present:     []string{`b:*x*`},
			wantAST:     `(not c:2)`,
			wantLookups: []string{`c:2`},
		},
		{
			name:        "or of empty branches is empty",
			query:       `a:1 or b:2`,
			wantEmpty:   true,
			wantLookups: []string{`a:1`, `b:2`},
		},
		{
			name:        "empty negative part of nand is dropped",
			query:       `a:1 and not b:2`,
			present:     []string{`a:1`},
			wantAST:     `a:1`,
			wantLookups: []string{`a:1`, `b:2`},
		},
		{
			name:        "empty regular part of nand skips negative part",
			query:       `not a:1 and b:2`,
			present:     []string{`a:1`},
			wantEmpty:   true,
			wantLookups: []string{`b:2`},
		},
		{
			name:        "not of empty is not empty",
			query:       `not a:1`,
			wantAST:     `(not a:1)`,
			wantLookups: []string{`a:1`},
		},
		{
			name:        "all present keeps the tree",
			query:       `a:1 and (b:2 or c:*3)`,
			present:     []string{`a:1`, `b:2`, `c:*3`},
			wantAST:     `(a:1 and (b:2 or c:*3))`,
			wantLookups: []string{`a:1`, `b:2`, `c:*3`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.ParseSeqQL(tt.query, nil)
			require.NoError(t, err)

			before := astDump(q.Root)
			index := &recordingTokenIndex{present: map[string]bool{}}
			for _, p := range tt.present {
				index.present[p] = true
			}

			got, empty, err := astResolve(q.Root, index, make(tidsCache), stopwatch.New())
			require.NoError(t, err)

			assert.Equal(t, tt.wantEmpty, empty)
			if !tt.wantEmpty {
				assert.Equal(t, tt.wantAST, astDump(got))
			}

			assert.Equal(t, tt.wantLookups, index.lookups)
			assert.Equal(t, before, astDump(q.Root), "shared AST must not be mutated")
		})
	}
}

func TestASTResolverOrder(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		freqs   map[uint32]uint32
		wantAST string
	}{
		{
			name:    "rarest conjunct goes first",
			query:   `a:1 and b:2 and c:3`,
			freqs:   map[uint32]uint32{1: 100, 2: 10, 3: 0},
			wantAST: `((c:3 and b:2) and a:1)`,
		},
		{
			name:    "equal sizes keep query order",
			query:   `c:3 and (b:2 and a:1)`,
			wantAST: `((c:3 and b:2) and a:1)`,
		},
		{
			name:    "or size is the sum of its branches",
			query:   `(a:1 or b:2) and c:3`,
			freqs:   map[uint32]uint32{1: 5, 2: 5, 3: 7},
			wantAST: `(c:3 and (a:1 or b:2))`,
		},
		{
			name:    "smaller or stays first",
			query:   `(a:1 or b:2) and c:3`,
			freqs:   map[uint32]uint32{1: 5, 2: 5, 3: 20},
			wantAST: `((a:1 or b:2) and c:3)`,
		},
		{
			name:    "regular part of nand is reordered",
			query:   `a:1 and b:2 and not c:3`,
			freqs:   map[uint32]uint32{1: 100, 2: 10, 3: 1},
			wantAST: `(not c:3 and (b:2 and a:1))`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.ParseSeqQL(tt.query, nil)
			require.NoError(t, err)
			before := astDump(q.Root)

			index := &testTokenIndex{
				tids:  map[string][]uint32{"a:1": {1}, "b:2": {2}, "c:3": {3}},
				freqs: tt.freqs,
			}

			got, empty, err := astResolve(q.Root, index, make(tidsCache), stopwatch.New())
			require.NoError(t, err)
			require.False(t, empty)

			assert.Equal(t, tt.wantAST, astDump(got))
			assert.Equal(t, before, astDump(q.Root), "shared AST must not be mutated")
		})
	}
}

func astDump(n *parser.ASTNode) string {
	var b strings.Builder
	n.DumpSeqQL(&b)
	return b.String()
}
