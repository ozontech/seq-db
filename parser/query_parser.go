package parser

import (
	"fmt"
	"strings"

	"github.com/ozontech/seq-db/seq"
)

type queryParser struct {
	tokenParser
	mapping seq.Mapping
}

var builtinIndexes = map[string]seq.TokenizerType{
	seq.TokenAll:    seq.TokenizerTypeKeyword,
	seq.TokenExists: seq.TokenizerTypeKeyword,
	seq.TokenIndex:  seq.TokenizerTypeKeyword,
}

func (qp *queryParser) indexType(field string) (seq.TokenizerType, error) {
	if qp.mapping == nil {
		return seq.TokenizerTypeKeyword, nil
	}
	tokKinds, has := qp.mapping[field]
	if has {
		return tokKinds.Main.TokenizerType, nil
	}
	tokKind, has := builtinIndexes[field]
	if has {
		return tokKind, nil
	}
	return seq.TokenizerTypeNoop, fmt.Errorf(`unindexed field "%s"`, field)
}

// parseSubexpr parses subexpression, delimited by AND, OR, NOT or enclosing round bracket
// i.e. either `token`, `NOT subexpr` or `(expr)`
func (qp *queryParser) parseSubexpr(depth int) (*ASTNode, error) {
	if qp.eof() {
		return nil, qp.errorEOF("token expression")
	}
	if qp.cur() == '(' {
		qp.pos++
		qp.skipSpaces()
		expr, err := qp.parseExpr(depth + 1)
		if err != nil {
			return nil, err
		}
		if qp.eof() {
			return nil, qp.errorEOF("closing round bracket ')'")
		}
		if qp.cur() != ')' {
			return nil, qp.errorUnexpectedSymbol("in place of closing round bracket ')'")
		}
		qp.pos++
		qp.skipSpaces()
		return expr, nil
	}
	pos := qp.pos
	fieldName := qp.parseSimpleTerm()
	if strings.EqualFold(fieldName, "not") {
		child, err := qp.parseSubexpr(depth)
		if err != nil {
			return nil, err
		}
		return newNotNode(child), nil
	}
	if fieldName == "" {
		return nil, qp.errorUnexpectedSymbol("in place of field name")
	}
	indexType, err := qp.indexType(fieldName)
	if err != nil {
		qp.pos = pos
		return nil, qp.errorWrap(err)
	}
	tokens, err := qp.parseTokenQuery(fieldName, indexType)
	if err != nil {
		return nil, err
	}
	return buildAndTree(tokens), nil
}

// parseExpr parses higher-lever expressions, recursively calling parseSubexpr
// i.e. it parses `subexpr { AND/OR suboexpr }...`
// `AND has higher priority, i.e. `a:a OR b:b AND c:c` = `a:a OR (b:b AND c:c)`
// left operation have higher priority, i.e. `a:a AND b:b AND c:c` = `(a:a AND b:b) AND c:c`
// actually implements simplified Dijkstra algorithm, and can be replaced with one if needed
func (qp *queryParser) parseExpr(depth int) (*ASTNode, error) {
	leftHigh, err := qp.parseSubexpr(depth) // left operand of AND, of high priority
	if err != nil {
		return nil, err
	}
	var leftLow *ASTNode // left operand of OR, of low priority
	for {
		pos := qp.pos
		operator := qp.parseSimpleTerm()
		var opKind logicalKind
		switch strings.ToLower(operator) {
		case "and":
			opKind = LogicalAnd
		case "or":
			opKind = LogicalOr
		case "":
			if qp.eof() || (qp.cur() == ')' && depth > 0) {
				if leftLow != nil && leftHigh != nil {
					return newLogicalNode(LogicalOr, leftLow, leftHigh), nil
				}
				return leftHigh, nil
			}
			return nil, qp.errorUnexpectedSymbol(`instead of operator (only "and", "or" and "not" are supported)`)
		default:
			return nil, qp.errorUnexpected(pos, `operator "%s" (only "and"/"or" are supported here)`, operator)
		}
		right, err := qp.parseSubexpr(depth)
		if err != nil {
			return nil, err
		}
		if opKind == LogicalAnd {
			// leftLow OR leftHigh AND right = leftLow OR (leftHigh AND right)
			// no need to touch leftLow
			leftHigh = newLogicalNode(LogicalAnd, leftHigh, right)
		} else {
			if leftLow == nil {
				// leftHigh can no longer be amended with `AND`
				leftLow = leftHigh
			} else {
				// leftLow OR leftHigh OR right = (leftLow OR leftHigh) OR right
				// just fold
				leftLow = newLogicalNode(LogicalOr, leftLow, leftHigh)
			}
			// it can always be followed by `AND`
			leftHigh = right
		}
	}
}

func buildAst(data string, mapping seq.Mapping) (*ASTNode, error) {
	p := queryParser{
		tokenParser: tokenParser{
			data: []rune(data),
		},
		mapping: mapping,
	}
	p.skipSpaces()
	return p.parseExpr(0)
}

func ParseQuery(data string, mapping seq.Mapping) (*ASTNode, error) {
	root, err := buildAst(data, mapping)
	if err != nil {
		return nil, err
	}
	root, not := propagateNot(root)
	if not {
		return newNotNode(root), nil
	}
	return root, nil
}

func ParseSingleTokenForTests(name, data string) (Token, error) {
	p := tokenParser{
		data: []rune(data),
	}
	p.skipSpaces()
	if p.eof() {
		return nil, p.errorEOF("need literal")
	}
	tokens, err := p.parseLiteral(name, seq.TokenizerTypeKeyword)
	if err != nil {
		return nil, err
	}
	if len(tokens) != 1 {
		return nil, fmt.Errorf("more than one token")
	}
	return tokens[0], nil
}

func ParseAggregationFilter(data string) (*Literal, error) {
	p := tokenParser{
		data: []rune(data),
	}
	p.skipSpaces()
	if p.eof() {
		return nil, nil
	}
	fieldName := p.parseSimpleTerm()
	if fieldName == "" {
		return nil, p.errorUnexpectedSymbol("in place of field name")
	}
	tokens, err := p.parseTokenQuery(fieldName, seq.TokenizerTypeKeyword)
	if err != nil {
		return nil, err
	}
	if !p.eof() {
		return nil, fmt.Errorf("too complex query for aggregation")
	}
	if len(tokens) != 1 {
		return nil, fmt.Errorf("too complex query for aggregation")
	}
	token, is := tokens[0].(*Literal)
	if !is {
		return nil, fmt.Errorf("too complex query for aggregation")
	}
	return token, nil
}
