package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tokenKeys parses the query twice and returns TokenKey of every leaf token,
// so the test never reuses AST nodes between the two parses.
func tokenKeys(t *testing.T, q string) []string {
	t.Helper()

	keys := make([]string, 0, 2)
	for range 2 {
		query, err := ParseSeqQL(q, nil)
		require.NoError(t, err)

		for _, node := range collectLeaves(query.Root) {
			keys = append(keys, TokenKey(node.Value))
		}
	}
	return keys
}

func collectLeaves(root *ASTNode) []*ASTNode {
	if len(root.Children) == 0 {
		return []*ASTNode{root}
	}
	var leaves []*ASTNode
	for _, child := range root.Children {
		leaves = append(leaves, collectLeaves(child)...)
	}
	return leaves
}

func TestTokenKeyStableAcrossParses(t *testing.T) {
	a := assert.New(t)

	tests := []struct {
		name  string
		query string
	}{
		{"literal", `service:api-gateway`},
		{"wildcard suffix", `msg:*timeout`},
		{"wildcard contains", `msg:*connection*refused*`},
		{"wildcard prefix", `msg:timeout*`},
		{"exists", `service:*`},
		{"range", `msec:[100 to 200)`},
		{"ip range cidr", `host:ip_range(192.168.0.0/24)`},
		{"ip range pair", `host:ip_range(192.168.0.1, 192.168.0.255)`},
		{"re", `host:re("seqdb-(stg|prod)-[0-9]+")`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keys := tokenKeys(t, tt.query)
			a.NotEmpty(keys)
			for _, key := range keys {
				a.Equal(keys[0], key)
			}
		})
	}
}

func TestTokenKeyDistinguishesSemantics(t *testing.T) {
	a := assert.New(t)

	tests := []struct {
		name string
		this string
		that string
	}{
		{"different fields", `a:b`, `c:b`},
		{"different values", `a:b`, `a:c`},
		{"suffix vs literal", `a:*b`, `a:b`},
		{"suffix vs contains", `a:*b`, `a:*b*`},
		{"different wildcard shape", `a:x*y*z`, `a:xy*z`},
		{"range bounds", `msec:[100 to 200)`, `msec:(100 to 200]`},
		{"range vs ip range", `host:[a to b]`, `host:ip_range(1.1.1.1, 2.2.2.2)`},
		{"ip ranges", `host:ip_range(1.1.1.0/24)`, `host:ip_range(1.1.2.0/24)`},
		{"re expressions", `host:re("a+b")`, `host:re("a*b")`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a.NotEqual(
				TokenKey(leaf(t, tt.this)),
				TokenKey(leaf(t, tt.that)),
			)
		})
	}
}

func leaf(t *testing.T, q string) Token {
	t.Helper()

	query, err := ParseSeqQL(q, nil)
	require.NoError(t, err)
	return collectLeaves(query.Root)[0].Value
}

// Dumped tokens must never collide: quoting of fields and values that
// contain special characters is what keeps `"a:b":c` and `a:"b:c"` distinct.
func TestTokenKeyNoConcatenationCollisions(t *testing.T) {
	a := assert.New(t)
	a.NotEqual(
		TokenKey(&Literal{Field: "a", Terms: []Term{{Kind: TermText, Data: "b:c"}}}),
		TokenKey(&Literal{Field: "a:b", Terms: []Term{{Kind: TermText, Data: "c"}}}),
	)
}
