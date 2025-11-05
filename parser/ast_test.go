package parser

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildingTree(t *testing.T) {
	a := assert.New(t)

	query, err := parse(`a:a OR b:b AND NOT c:c`, nil)
	a.NoError(err)
	fmt.Println(query.SeqQLString())

	act := query.Root
	a.Equal(LogicalOr, act.Value.(*Logical).Operator)
	a.Equal(2, len(act.Children))
	a.Equal("a:a", act.Children[0].SeqQLString())
	a.Equal(0, len(act.Children[0].Children))
	a.Equal(LogicalAnd, act.Children[1].Value.(*Logical).Operator)
	a.Equal(2, len(act.Children[1].Children))
	a.Equal("b:b", act.Children[1].Children[0].SeqQLString())
	a.Equal(0, len(act.Children[1].Children[0].Children))
	a.Equal(LogicalNot, act.Children[1].Children[1].Value.(*Logical).Operator)
	a.Equal(1, len(act.Children[1].Children[1].Children))
	a.Equal("c:c", act.Children[1].Children[1].Children[0].SeqQLString())
	a.Equal(0, len(act.Children[1].Children[1].Children[0].Children))
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
