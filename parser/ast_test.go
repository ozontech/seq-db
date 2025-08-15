package parser

import (
	"fmt"
	"math/rand"
)

// TODO(moflotas): understand, why fails
//func TestBuildingTree(t *testing.T) {
//	query, err := ParseSeqQL(`a:a OR b:b AND NOT c:c`, nil)
//	assert.NoError(t, err)
//	fmt.Println(query.SeqQLString())
//
//	act := query.Root
//	assert.Equal(t, LogicalOr, act.Value.(*Logical).Operator)
//	assert.Equal(t, 2, len(act.Children))
//	assert.Equal(t, "a:a", act.Children[0].Value.(*Literal).String())
//	assert.Equal(t, 0, len(act.Children[0].Children))
//	assert.Equal(t, LogicalAnd, act.Children[1].Value.(*Logical).Operator)
//	assert.Equal(t, 2, len(act.Children[1].Children))
//	assert.Equal(t, "b:b", act.Children[1].Children[0].Value.(*Literal).String())
//	assert.Equal(t, 0, len(act.Children[1].Children[0].Children))
//	assert.Equal(t, LogicalNot, act.Children[1].Children[1].Value.(*Logical).Operator)
//	assert.Equal(t, 1, len(act.Children[1].Children[1].Children))
//	assert.Equal(t, "c:c", act.Children[1].Children[1].Children[0].Value.(*Literal).String())
//	assert.Equal(t, 0, len(act.Children[1].Children[1].Children[0].Children))
//}

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
