package parser

import (
	"strings"
)

type ASTNode struct {
	Children []*ASTNode
	Value    Token
}

func newTokenNode(token Token) *ASTNode {
	return &ASTNode{Value: token}
}

func newNotNode(nested *ASTNode) *ASTNode {
	return &ASTNode{
		Children: []*ASTNode{nested},
		Value:    &Logical{LogicalNot},
	}
}

func newLogicalNode(operator logicalKind, left, right *ASTNode) *ASTNode {
	return &ASTNode{
		Children: []*ASTNode{left, right},
		Value:    &Logical{operator},
	}
}

func buildAndTree(tokens []Token) *ASTNode {
	tree := newTokenNode(tokens[0])
	for _, t := range tokens[1:] {
		tree = newLogicalNode(LogicalAnd, tree, newTokenNode(t))
	}
	return tree
}

func propagateNot(node *ASTNode) (*ASTNode, bool) {
	logical, is := node.Value.(*Logical)

	if !is {
		return node, false
	}

	if logical.Operator == LogicalNot {
		nested, not := propagateNot(node.Children[0])
		return nested, !not
	}

	left, leftNot := propagateNot(node.Children[0])
	right, rightNot := propagateNot(node.Children[1])

	not := false
	node.Children[0], node.Children[1] = left, right

	if logical.Operator == LogicalOr {
		if leftNot || rightNot {
			logical.Operator = LogicalAnd
			not = true
			// at least one becomes false
			leftNot = !leftNot
			rightNot = !rightNot
		} else {
			return node, false
		}
	}

	if leftNot && rightNot {
		logical.Operator = LogicalOr
		// `not` is false
		return node, true
	}

	if leftNot {
		logical.Operator = LogicalNAnd
	}

	if rightNot {
		node.Children[0], node.Children[1] = right, left // sic!
		logical.Operator = LogicalNAnd
	}

	return node, not
}

// Dump is used in tests only
func (e *ASTNode) Dump(builder *strings.Builder) {
	switch t := e.Value.(type) {
	case *Logical:
		builder.WriteByte('(')
		switch t.Operator {
		case LogicalNot:
			builder.WriteString("NOT ")
			e.Children[0].Dump(builder)
		case LogicalNAnd:
			builder.WriteString("NOT ")
			fallthrough
		case LogicalOr, LogicalAnd:
			e.Children[0].Dump(builder)
			if t.Operator == LogicalOr {
				builder.WriteString(" OR ")
			} else {
				builder.WriteString(" AND ")
			}
			e.Children[1].Dump(builder)
		default:
			panic("unknown operator")
		}
		builder.WriteByte(')')
	case *Literal:
		t.Dump(builder)
	case *Range:
		t.Dump(builder)
	default:
		panic("unknown token implementation")
	}
}

// String is used in tests only
func (e *ASTNode) String() string {
	builder := &strings.Builder{}
	e.Dump(builder)
	return builder.String()
}
