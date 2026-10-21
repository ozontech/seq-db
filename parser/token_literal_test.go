package parser

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/seq"
)

func TestIsMatchAll(t *testing.T) {
	mustParse := func(t *testing.T, query string) Token {
		t.Helper()
		q, err := ParseSeqQL(query, nil)
		require.NoError(t, err)
		require.NotNil(t, q.Root)
		return q.Root.Value
	}

	wildcard := []Term{{Kind: TermSymbol, Data: "*"}}
	emptyText := []Term{{Kind: TermText, Data: ""}}

	tests := []struct {
		name  string
		token Token
		want  bool
	}{
		{"nil", nil, false},
		{"bare asterisk query", mustParse(t, "*"), true},
		{"empty query", mustParse(t, ""), true},
		{"explicit _all_:*", &Literal{Field: seq.TokenAll, Terms: wildcard}, true},
		{"_all_ with empty text", &Literal{Field: seq.TokenAll, Terms: emptyText}, true},
		{"_all_ with several terms", &Literal{Field: seq.TokenAll, Terms: append(wildcard, emptyText...)}, false},
		{"_all_ with non-empty text", &Literal{Field: seq.TokenAll, Terms: []Term{{Kind: TermText, Data: "foo"}}}, false},
		{"other field wildcard", &Literal{Field: "level", Terms: wildcard}, false},
		{"other field empty text", &Literal{Field: "level", Terms: emptyText}, false},
		{"_exists_ wildcard", &Literal{Field: seq.TokenExists, Terms: wildcard}, false},
		{"non-literal token", &Range{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsMatchAll(tt.token))
		})
	}
}
