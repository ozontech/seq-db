package parser

import (
	"regexp/syntax"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOptimizationPass(t *testing.T) {
	parse := func(s string) *syntax.Regexp {
		t.Helper()

		re, err := syntax.Parse(s, syntax.Perl)
		require.NoError(t, err)

		return re
	}

	cases := []struct {
		re       string
		expected string
	}{
		{
			// Remove capturing groups.
			re:       "(a(b(c(d))))",
			expected: "abcd",
		},
		{
			re: "(seqdb-prod)?",
			// This is non-capturing group.
			expected: "(?:seqdb-prod)?",
		},
		{
			re: "[1][2][3]",
			// This is non-capturing group.
			expected: "123",
		},
	}

	for _, cc := range cases {
		assert.Equal(t, cc.expected, optimizeRe(parse(cc.re)).String())
	}
}

func TestLiteralExtraction(t *testing.T) {
	parse := func(s string) *syntax.Regexp {
		t.Helper()

		re, err := syntax.Parse(s, syntax.Perl)
		require.NoError(t, err)

		return optimizeRe(re)
	}

	prefix := func(re *syntax.Regexp) []ReLiteral {
		if r := prefix(re); len(r.Value) > 0 {
			return []ReLiteral{r}
		}
		return []ReLiteral{}
	}

	suffix := func(re *syntax.Regexp) []ReLiteral {
		if r := suffix(re); len(r.Value) > 0 {
			return []ReLiteral{r}
		}
		return []ReLiteral{}
	}

	cases := []struct {
		re       *syntax.Regexp
		expected []ReLiteral
		fn       func(*syntax.Regexp) []ReLiteral
	}{
		{
			re:       parse("simple"),
			expected: []ReLiteral{{Value: []byte("simple"), Foldable: false}},
			fn:       prefix,
		},
		{
			re:       parse("easy-prefix-[a-zA-Z]"),
			expected: []ReLiteral{{Value: []byte("easy-prefix-"), Foldable: false}},
			fn:       prefix,
		},
		{
			re:       parse("(video|vodka)-prefix-[a-zA-Z]"),
			expected: []ReLiteral{{Value: []byte("v"), Foldable: false}},
			fn:       prefix,
		},
		{
			re:       parse("(?i)prefix-[a-zA-Z]"),
			expected: []ReLiteral{{Value: []byte("PREFIX-"), Foldable: true}},
			fn:       prefix,
		},
		{
			re:       parse("(no|prefix)-suffix"),
			expected: []ReLiteral{},
			fn:       prefix,
		},

		{
			re:       parse("simple"),
			expected: []ReLiteral{{Value: []byte("simple"), Foldable: false}},
			fn:       suffix,
		},
		{
			re:       parse("((a)b)"),
			expected: []ReLiteral{{Value: []byte("ab"), Foldable: false}},
			fn:       suffix,
		},
		{
			re:       parse("prefix-[a-zA-Z]-suffix"),
			expected: []ReLiteral{{Value: []byte("-suffix"), Foldable: false}},
			fn:       suffix,
		},
		{
			re:       parse("suffix-(no|literal)"),
			expected: []ReLiteral{},
			fn:       suffix,
		},

		{
			re: parse("(something|nothing)-in-(a|the)-way-(song)?"),
			expected: []ReLiteral{
				{Value: []byte("-in-"), Foldable: false},
				{Value: []byte("-way-"), Foldable: false},
			},
			fn: middle,
		},
		{
			re: parse("([a-z]+-one-([0-9]-two))-three"),
			expected: []ReLiteral{
				{Value: []byte("-one-"), Foldable: false},
				{Value: []byte("-two-three"), Foldable: false},
			},
			fn: middle,
		},
	}

	for _, cc := range cases {
		assert.Equal(t, cc.expected, cc.fn(cc.re))
	}
}
