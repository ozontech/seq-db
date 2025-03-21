package parser

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type astTest struct {
	name  string
	query string
	exp   string
}

func TestParsingAST(t *testing.T) {
	tests := []astTest{
		{
			name:  `simple_0`,
			query: `service: composer-api`,
			exp:   `service:composer-api`,
		},
		{
			name:  `simple_1`,
			query: `  s    : a   OR   l     :   3  `,
			exp:   `(s:a OR l:3)`,
		},
		{
			name:  `simple_2`,
			query: `s: a OR l: 3 AND q:b`,
			exp:   `(s:a OR (l:3 AND q:b))`,
		},
		{
			name:  `simple_3`,
			query: `s: a OR l: 3 OR q:b`,
			exp:   `((s:a OR l:3) OR q:b)`,
		},
		{
			name:  `simple_4`,
			query: ` NOT  s : a `,
			exp:   `(NOT s:a)`,
		},
		{
			name:  `simple_5`,
			query: `s:a OR NOT s:b OR s:c`,
			exp:   `((s:a OR (NOT s:b)) OR s:c)`,
		},
		{
			name:  `simple_6`,
			query: `NOT (s:a OR s:c)`,
			exp:   `(NOT (s:a OR s:c))`,
		},
		{
			name:  `simple_7`,
			query: `NOT NOT s:a`,
			exp:   `(NOT (NOT s:a))`,
		},
		{
			name:  `wildcard_0`,
			query: `service:*`,
			exp:   `service:*`,
		},
		{
			name:  `wildcard_1`,
			query: ` service : * `,
			exp:   `service:*`,
		},
	}
	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			act, err := buildAst(tst.query, nil)
			require.NoError(t, err)

			genStr := act.String()
			assert.Equal(t, tst.exp, genStr)
			second, err := buildAst(genStr, nil)
			require.NoError(t, err)
			assert.Equal(t, genStr, second.String())
		})
	}
}

func TestBuildingTree(t *testing.T) {
	act, err := buildAst(`a:a OR b:b AND NOT c:c`, nil)
	assert.NoError(t, err)
	assert.Equal(t, LogicalOr, act.Value.(*Logical).Operator)
	assert.Equal(t, 2, len(act.Children))
	assert.Equal(t, "a:a", act.Children[0].Value.(*Literal).String())
	assert.Equal(t, 0, len(act.Children[0].Children))
	assert.Equal(t, LogicalAnd, act.Children[1].Value.(*Logical).Operator)
	assert.Equal(t, 2, len(act.Children[1].Children))
	assert.Equal(t, "b:b", act.Children[1].Children[0].Value.(*Literal).String())
	assert.Equal(t, 0, len(act.Children[1].Children[0].Children))
	assert.Equal(t, LogicalNot, act.Children[1].Children[1].Value.(*Logical).Operator)
	assert.Equal(t, 1, len(act.Children[1].Children[1].Children))
	assert.Equal(t, "c:c", act.Children[1].Children[1].Children[0].Value.(*Literal).String())
	assert.Equal(t, 0, len(act.Children[1].Children[1].Children[0].Children))
}

func tLogical(t logicalKind) Token {
	return &Logical{Operator: t}
}

func tToken(field string, terms ...Term) Token {
	return &Literal{Field: field, Terms: terms}
}

func tText(data string) Term {
	return Term{Kind: TermText, Data: data}
}

func addOperator(e *ASTNode, cnt int) {
	if len(e.Children) == 0 {
		var kind logicalKind
		switch rand.Intn(3) {
		case 0:
			kind = LogicalOr
		case 1:
			kind = LogicalAnd
		case 2:
			kind = LogicalNot
		}
		e.Value = tLogical(kind)
		left := newTokenNode(tToken("m", tText(fmt.Sprint(cnt+1))))
		e.Children = append(e.Children, left)
		if kind != LogicalNot {
			right := newTokenNode(tToken("m", tText(fmt.Sprint(cnt+2))))
			e.Children = append(e.Children, right)
		}
		return
	}
	addOperator(e.Children[rand.Intn(len(e.Children))], cnt)
}

func checkSelf(t *testing.T, e *ASTNode) {
	q := e.String()
	exp, err := buildAst(q, nil)
	require.NoError(t, err)
	require.Equal(t, q, exp.String())
}

func TestParsingASTStress(t *testing.T) {
	iterations := 500
	if testing.Short() {
		iterations = 50
	}
	rand.Seed(14444323)
	for i := 0; i < iterations; i++ {
		exp := &ASTNode{}
		for i := 0; i < 100; i++ {
			addOperator(exp, 2*i)
			checkSelf(t, exp)
		}
	}
}
