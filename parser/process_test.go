package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/seq"
)

type testCase struct {
	name   string
	query  string
	expect string
}

func TestAll(t *testing.T) {
	tests := []testCase{
		{
			name:   `simple_0`,
			query:  `service:some`,
			expect: `service:some`,
		},
		{
			name:   `simple_1`,
			query:  `service:"some text"`,
			expect: `service:some\ text`,
		},
		{
			name:   `simple_2`,
			query:  `text:"some text"`,
			expect: `(text:some AND text:text)`,
		},
		{
			name:   `simple_3`,
			query:  `text:"some very long text"`,
			expect: `(((text:some AND text:very) AND text:long) AND text:text)`,
		},
		{
			name:   `simple_4`,
			query:  `text:"a b" AND text:"c d f" OR text:"e f"`,
			expect: `(((text:a AND text:b) AND ((text:c AND text:d) AND text:f)) OR (text:e AND text:f))`,
		},
		{
			name:   `wildcard_0`,
			query:  `service:some*`,
			expect: `service:some*`,
		},
		{
			name:   `wildcard_1`,
			query:  `service:some*thing`,
			expect: `service:some*thing`,
		},
		{
			name:   `wildcard_2`,
			query:  `service:some*thing*`,
			expect: `service:some*thing*`,
		},
		{
			name:   `wildcard_3`,
			query:  `service:*thing*`,
			expect: `service:*thing*`,
		},
		{
			name:   `wildcard_4`,
			query:  `service:*`,
			expect: `service:*`,
		},
		{
			name:   `wildcard_5`,
			query:  `text:some*thing`,
			expect: `text:some*thing`,
		},
		{
			name:   `wildcard_6`,
			query:  `text:a**b**`,
			expect: `((text:a* AND text:*b*) AND text:*)`,
		},
		{
			name:   `range_0`,
			query:  `level:[1 TO 3]`,
			expect: `level:[1 TO 3]`,
		},
		{
			name:   `range_1`,
			query:  `level:{1 TO 3}`,
			expect: `level:{1 TO 3}`,
		},
		{
			name:   `range_2`,
			query:  `level:[* TO *]`,
			expect: `level:[* TO *]`,
		},
		{
			name:   `range_3`,
			query:  `level:[abc TO cbd]`,
			expect: `level:[abc TO cbd]`,
		},
		{
			name:   `range_4`,
			query:  `service:some AND level:[1 TO 3] AND level:[3 TO 5]`,
			expect: `((service:some AND level:[1 TO 3]) AND level:[3 TO 5])`,
		},
	}
	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			expr, err := ParseQuery(tst.query, seq.TestMapping)
			require.NoError(t, err)
			assert.Equal(t, tst.expect, expr.String())
		})
	}
}

func TestTokenization(t *testing.T) {
	tests := []testCase{
		{
			name:   `token_0`,
			query:  `service:abc`,
			expect: `service:abc`,
		},
		{
			name:   `token_1`,
			query:  `service:"quoted"`,
			expect: `service:quoted`,
		},
		{
			name:   `token_2`,
			query:  `service:"quoted spaces"`,
			expect: `service:quoted\ spaces`,
		},
		{
			name:   `token_3`,
			query:  `service:\"symbols\"`,
			expect: `service:\"symbols\"`,
		},
		{
			name:   `token_4`,
			query:  `message:"[1 TO 3]"`,
			expect: `message:\[1\ to\ 3\]`,
		},
		{
			name:   `token_5`,
			query:  `  message  :   hi  `,
			expect: `message:hi`,
		},
		{
			name:   `token_6`,
			query:  `MiXeD_CaSe:TeSt`,
			expect: `MiXeD_CaSe:test`,
		},
		{
			name:   `token_7`,
			query:  `MiXeD_CaSe:"TeSt"`,
			expect: `MiXeD_CaSe:test`,
		},
		{
			name:   `token_8`,
			query:  `service:""`,
			expect: `service:""`,
		},
		{
			name:   `wildcard_0`,
			query:  `service:cms*`,
			expect: `service:cms*`,
		},
		{
			name:   `wildcard_1`,
			query:  `service:cms*api`,
			expect: `service:cms*api`,
		},
		{
			name:   `wildcard_2`,
			query:  `service:cms*inter*api`,
			expect: `service:cms*inter*api`,
		},
		{
			name:   `wildcard_3`,
			query:  `service:"cms*inter*api"`,
			expect: `service:cms*inter*api`,
		},
		{
			name:   `wildcard_4`,
			query:  `service:"cms* inter* *api"`,
			expect: `service:cms*\ inter*\ *api`,
		},
		{
			name:   `range_0`,
			query:  `level:[1 to 3]`,
			expect: `level:[1 TO 3]`,
		},
		{
			name:   `range_1`,
			query:  `level:[* to 3]`,
			expect: `level:[* TO 3]`,
		},
		{
			name:   `range_2`,
			query:  `level:{1 to *]`,
			expect: `level:{1 TO *]`,
		},
		{
			name:   `range_3`,
			query:  `level:[1 to 3] AND id:[* TO "*"]`,
			expect: `(level:[1 TO 3] AND id:[* TO *])`,
		},
		{
			name:   `range_4`,
			query:  `level:["from" to "to"]`,
			expect: `level:[from TO to]`,
		},
		{
			name:   `range_5`,
			query:  `level:[from to to]`,
			expect: `level:[from TO to]`,
		},
		{
			name:   `range_6`,
			query:  `level:["a b c" to "d e f"]`,
			expect: `level:[a\ b\ c TO d\ e\ f]`,
		},
		{
			name:   `range_7`,
			query:  `level:["hi" to "ho"]`,
			expect: `level:[hi TO ho]`,
		},
		{
			name:   `range_8`,
			query:  `level:[-123 to -456]`,
			expect: `level:[-123 TO -456]`,
		},
		{
			name:   `range_9`,
			query:  `  level  :  [  1  to  3  ]  `,
			expect: `level:[1 TO 3]`,
		},
		{
			name:   `range_10`,
			query:  `level:["" to "a\*b"]`,
			expect: `level:["" TO a\*b]`,
		},
		{
			name:   `complex_0`,
			query:  `id:[-3 to 6} OR (message:"hel lo" AND level:[1 to 3])`,
			expect: `(id:[-3 TO 6} OR (message:hel\ lo AND level:[1 TO 3]))`,
		},
		{
			name:   `special_escaping_for_graylog_links_0`,
			query:  `level:  foo\-bar-baz-\/ban`,
			expect: `level:foo-bar-baz-/ban`,
		},
		{
			name:   `special_escaping_for_graylog_links_1`,
			query:  `level:  "foo\-bar-baz-\/ban"`,
			expect: `level:foo\\-bar-baz-\\/ban`,
		},
		{
			name:   `quotes_0`,
			query:  `level:"\"foo\"bar\"\\"`,
			expect: `level:\"foo\"bar\"\\`,
		},
	}
	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			expr, err := ParseQuery(tst.query, nil)
			require.NoError(t, err)
			assert.Equal(t, tst.expect, expr.String())
		})
	}
}

func TestTokenizationCaseSensitive(t *testing.T) {
	tests := []testCase{
		{
			name:   `case_0`,
			query:  `service:AbCdEf`,
			expect: `service:AbCdEf`,
		},
		{
			name:   `case_0`,
			query:  `service:"AbC"`,
			expect: `service:AbC`,
		},
	}
	config.CaseSensitive = true
	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			expr, err := ParseQuery(tst.query, nil)
			require.NoError(t, err)
			assert.Equal(t, tst.expect, expr.String())
		})
	}
}

func TestExistsCaseSensitive(t *testing.T) {
	q := `_exists_:AbCdEfG`
	for _, cs := range []bool{true, false} {
		config.CaseSensitive = cs
		expr, err := ParseQuery(q, nil)
		assert.NoError(t, err)
		assert.Equal(t, expr.String(), q)
	}
}

func TestParseRange(t *testing.T) {
	expr, err := ParseQuery(`level:{1 TO *]`, seq.TestMapping)
	require.NoError(t, err)
	_, is := expr.Value.(*Range)
	require.True(t, is)
	require.IsType(t, &Range{}, expr.Value)
	r := expr.Value.(*Range)
	assert.Equal(t, "level", r.Field)
	assert.Equal(t, false, r.IncludeFrom)
	assert.Equal(t, true, r.IncludeTo)
	assert.Equal(t, TermText, r.From.Kind)
	assert.Equal(t, TermSymbol, r.To.Kind)
	assert.Equal(t, "1", r.From.Data)
	assert.Equal(t, "*", r.To.Data)
}

func TestPropagateNot(t *testing.T) {
	tests := []testCase{
		{
			name:   `double_not`,
			query:  `NOT NOT m:a`,
			expect: `m:a`,
		},
		{
			name:   `and_double_not`,
			query:  `m:a AND NOT NOT m:b`,
			expect: `(m:a AND m:b)`,
		},
		{
			name:   `nand`,
			query:  `m:a AND NOT m:b`,
			expect: `(NOT m:b AND m:a)`,
		},
		{
			name:   `or_double_not`,
			query:  `NOT NOT m:a OR m:b`,
			expect: `(m:a OR m:b)`,
		},
		{
			name:   `nor`,
			query:  `NOT m:a OR m:b`,
			expect: `(NOT (NOT m:b AND m:a))`,
		},
		{
			name:   `propagate_and`,
			query:  `NOT (NOT m:a AND NOT m:b)`,
			expect: `(m:a OR m:b)`,
		},
		{
			name:   `or_tree_left`,
			query:  `NOT m:a OR m:b OR m:c OR m:d`,
			expect: `(NOT (NOT m:d AND (NOT m:c AND (NOT m:b AND m:a))))`,
		},
		{
			name:   `or_tree_right`,
			query:  `m:a OR m:b OR m:c OR NOT m:d`,
			expect: `(NOT (NOT ((m:a OR m:b) OR m:c) AND m:d))`,
		},
		{
			name:   `and_tree_left`,
			query:  `NOT m:a AND m:b AND m:c AND m:d`,
			expect: `(((NOT m:a AND m:b) AND m:c) AND m:d)`,
		},
		{
			name:   `and_tree_right`,
			query:  `m:a AND m:b AND m:c AND NOT m:d`,
			expect: `(NOT m:d AND ((m:a AND m:b) AND m:c))`,
		},
		{
			name:   `big_tree`,
			query:  `NOT ((NOT m:a OR (NOT m:b AND m:c)) AND (NOT m:d AND NOT m:e))`,
			expect: `((NOT (NOT m:b AND m:c) AND m:a) OR (m:d OR m:e))`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node, err := ParseQuery(test.query, nil)
			require.NoError(t, err)
			assert.Equal(t, test.expect, node.String())
		})
	}
}

func TestWildcardText(t *testing.T) {
	tests := []testCase{
		{
			query:  `text:"some* weird* *cases"`,
			expect: `((text:some* AND text:weird*) AND text:*cases)`,
		},
		{
			query:  `text:"some *weird cases* hmm very*intrs"`,
			expect: `((((text:some AND text:*weird) AND text:cases*) AND text:hmm) AND text:very*intrs)`,
		},
		{
			query:  `text:value=* AND text:value=\** AND text:value=\*\** AND text:\*\* AND text:\*\**`,
			expect: `(((((text:value AND text:*) AND (text:value AND text:\**)) AND (text:value AND text:\*\**)) AND text:\*\*) AND text:\*\**)`,
		},
		{
			query:  `text:val* AND text:val\**`,
			expect: `(text:val* AND text:val\**)`,
		},
	}

	for _, test := range tests {
		t.Run("wildcard", func(t *testing.T) {
			ast, err := ParseQuery(test.query, seq.TestMapping)
			require.NoError(t, err)
			assert.Equal(t, test.expect, ast.String())
		})
	}
}
