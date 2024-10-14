package query

import (
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/pkg/errors"

	"github.com/ozontech/seq-db/conf"
	"github.com/ozontech/seq-db/util"
)

const (
	TokenAll    = "_all_"
	TokenExists = "_exists_"
	TokenIndex  = "_index"
)

var (
	ExistsTokenName = []byte(TokenExists)
	IndexTokenName  = []byte(TokenIndex)
	AllTokenName    = []byte(TokenAll)
	AllToken        = Token{Field: AllTokenName, Val: []byte("")}
)

type Parser struct {
	opsPool       []Op
	operandStack  []*Op
	operatorStack []*Op
	state         State
	tokenizer     *Tokenizer
	plan          Plan
}

type Plan struct {
	Query     []byte
	Ops       []*Op
	SourceOps []*Op
	Explain   bool
	DocID     uint64
}

type Op struct {
	Kind   Kind
	NextOp *Op
	IsNeg  bool

	Index     int
	PosChilds int
	NegChilds int
	Token     Token
}

type OpTIDs [][]uint32 // tids for each source op

type EvalOp struct {
	Capacity   int
	PosCounter int
	NegCounter int
}

type (
	Kind  int
	State int
)

const (
	KindNo Kind = iota
	KindSourceMatch
	KindSourceRange
	KindBracket
	KindAnd
	KindOr
	KindRoot
	KindNot
)

const (
	stateAny State = iota
	stateOperator
)

func (n Kind) String() string {
	switch n {
	case KindBracket:
		return "BRACKET"
	case KindAnd:
		return "AND"
	case KindOr:
		return "OR"
	case KindNo:
		return "NO"
	case KindSourceMatch:
		return "SOURCE_MATCH"
	case KindSourceRange:
		return "SOURCE_RANGE"
	case KindRoot:
		return "ROOT"
	case KindNot:
		return "NOT"
	}

	return "UNKNOWN"
}

func NewParser(mapping Mapping, maxTokenSize int) *Parser {
	return &Parser{
		operandStack:  make([]*Op, 0),
		operatorStack: make([]*Op, 0),
		opsPool:       make([]Op, 0, 32),
		state:         0,
		tokenizer:     NewTokenizer(maxTokenSize, conf.CaseSensitive, mapping),
	}
}

// ParseErr is an err that should be returned in case of wrong query syntax.
type ParseErr struct {
	err error
}

func (p ParseErr) Error() string {
	return p.err.Error()
}

func (i *Parser) Parse(query []byte) (*Plan, time.Duration, error) {
	startTime := time.Now()

	i.plan.SourceOps = i.plan.SourceOps[:0]
	i.opsPool = i.opsPool[:0]
	i.plan.Ops = i.plan.Ops[:0]
	i.plan.Query = query

	queryStr := util.ByteToStringUnsafe(query)
	queryStr = i.skipWC(queryStr)

	if len(queryStr) > 8 && queryStr[:8] == "EXPLAIN " {
		i.plan.Explain = true
		queryStr = queryStr[8:]
	}

	if queryStr == "EXPLAIN" {
		i.plan.Explain = true
		queryStr = ""
	}

	i.operandStack = i.operandStack[:0]
	i.operatorStack = i.operatorStack[:0]

	i.state = stateAny
	i.pushOperator(KindBracket)

	var err error
	for {
		queryStr, err = i.next(queryStr)

		if err != nil {
			var pErr ParseErr
			if errors.As(err, &pErr) {
				return nil, time.Since(startTime), errors.Wrapf(err, "tail=%s", queryStr)
			}
			return nil, time.Since(startTime), err
		}

		if queryStr == "" {
			break
		}
	}

	duration := time.Since(startTime)

	if i.topOperator() == KindNo {
		return nil, duration, fmt.Errorf("expected something, got nothing at the top operator")
	}
	if i.topOperator() != KindBracket {
		err := i.doStackedOp()
		if err != nil {
			return nil, duration, err
		}
	}

	if len(i.operandStack) == 0 {
		_, err := i.pushOperand(i.addSourceOperand(AllToken, KindSourceMatch))
		if err != nil {
			return nil, duration, err
		}
	}

	if len(i.operandStack) != 1 {
		return nil, duration, fmt.Errorf("wrong operator sequence")
	}

	op := i.getOp()
	op.Kind = KindRoot
	i.plan.Ops = append(i.plan.Ops, op)
	op.Index = len(i.plan.Ops) - 1

	i.operandStack[0].NextOp = op
	op.IsNeg = i.operandStack[0].IsNeg

	return &i.plan, duration, nil
}

func (i *Parser) skipWC(query string) string {
	if query == "" {
		return query
	}

	k := 0
	for _, c := range query {
		if !unicode.IsSpace(c) {
			break
		}
		k++
	}

	return query[k:]
}

func pref(s string, l int) string {
	if len(s) >= l {
		return s[:l]
	}

	return s
}

func (i *Parser) next(query string) (string, error) {
	query = i.skipWC(query)
	if query == "" {
		return query, nil
	}

	topOperator := i.topOperator()
	if topOperator == KindNo {
		return query, fmt.Errorf("expected something, got no operator")
	}
	if i.state == stateOperator {
		switch query[0] {
		case 'A':
			if pref(query, 4) != "AND " {
				return query, ParseErr{fmt.Errorf("unknown operator %q", pref(query, 4))}
			}
			i.state = stateAny
			if topOperator != KindBracket {
				err := i.doStackedOp()
				if err != nil {
					return query, err
				}
			}
			i.pushOperator(KindAnd)
			return query[4:], nil
		case 'O':
			if pref(query, 3) != "OR " {
				return query, ParseErr{fmt.Errorf("unknown operator %q", pref(query, 3))}
			}
			i.state = stateAny
			if topOperator != KindBracket {
				err := i.doStackedOp()
				if err != nil {
					return query, err
				}
			}
			i.pushOperator(KindOr)
			return query[3:], nil
		case ')':
			if topOperator != KindBracket {
				err := i.doStackedOp()
				if err != nil {
					return query, err
				}
			}
			if i.popOperator().Kind != KindBracket {
				return query, ParseErr{fmt.Errorf("broken brackets sequence")}
			}
			return query[1:], nil
		default:
			return query, ParseErr{fmt.Errorf("OR/AND operator expected")}
		}
	}

	switch query[0] {
	case 'N':
		if pref(query, 4) != "NOT " {
			return query, ParseErr{fmt.Errorf("unknown operator %q", pref(query, 4))}
		}
		i.pushOperator(KindNot)
		return query[3:], nil
	case '(':
		i.pushOperator(KindBracket)
		return query[1:], nil
	case ')':
		if topOperator != KindBracket {
			err := i.doStackedOp()
			if err != nil {
				return query, err
			}
		}
		if i.popOperator().Kind != KindBracket {
			return query, ParseErr{fmt.Errorf("broken brackets")}
		}
		return query[1:], nil
	default:
		return i.parseOperand(query)
	}
}

func (i *Parser) doStackedOp() error {
	op := i.popOperator()

	switch op.Kind {
	case KindOr, KindAnd:
		b := i.popOperand()
		a := i.popOperand()

		if a == nil || b == nil {
			return ParseErr{fmt.Errorf("broken operand sequence for %s operator", op.Kind)}
		}

		if a.IsNeg {
			op.NegChilds++
		} else {
			op.PosChilds++
		}

		if b.IsNeg {
			op.NegChilds++
		} else {
			op.PosChilds++
		}

		// DON'T deal with all negative operands.
		// using rule: !A AND !B == !(A OR B)
		kind := op.Kind
		if kind == KindAnd && op.PosChilds == 0 {
			op.IsNeg = true
			op.Kind = KindOr
			op.PosChilds = op.NegChilds
			op.NegChilds = 0
		}

		// ONLY deal with all positive operands.
		// using rule: !A OR !B == !(A AND B)
		if kind == KindOr && op.NegChilds != 0 {
			op.IsNeg = true
			op.Kind = KindAnd
			op.PosChilds = op.NegChilds
			op.NegChilds = 0
		}

		i.plan.Ops = append(i.plan.Ops, op)
		op.Index = len(i.plan.Ops) - 1
		a.NextOp = op
		b.NextOp = op
	case KindNot:
		a := i.popOperand()
		if a == nil {
			return ParseErr{fmt.Errorf("broken operand sequence for %s operator", op.Kind)}
		}

		i.plan.Ops = append(i.plan.Ops, op)
		op.Index = len(i.plan.Ops) - 1
		op.IsNeg = !a.IsNeg
		a.NextOp = op
	default:
		panic("unknown operator")
	}

	_, err := i.pushOperand(op)
	if err != nil {
		return err
	}

	return nil
}

func (i *Parser) topOperator() Kind {
	if len(i.operatorStack) == 0 {
		return KindNo
	}

	return i.operatorStack[len(i.operatorStack)-1].Kind
}

func (i *Parser) popOperand() *Op {
	l := len(i.operandStack) - 1
	if l < 0 {
		return nil
	}
	x := i.operandStack[l]
	i.operandStack = i.operandStack[:l]
	return x
}

func (i *Parser) popOperator() *Op {
	l := len(i.operatorStack) - 1
	x := i.operatorStack[l]
	i.operatorStack = i.operatorStack[:l]
	return x
}

func (i *Parser) getOp() *Op {
	i.opsPool = append(i.opsPool, Op{})
	return &i.opsPool[len(i.opsPool)-1]
}

func (i *Parser) addSourceOperand(token Token, kind Kind) *Op {
	op := i.getOp()
	op.Token = token
	op.Kind = kind

	i.plan.SourceOps = append(i.plan.SourceOps, op)
	op.Index = len(i.plan.SourceOps) - 1

	return op
}

func (i *Parser) pushOperand(opItem *Op) (*Op, error) {
	i.operandStack = append(i.operandStack, opItem)
	if i.topOperator() == KindNot {
		err := i.doStackedOp()
		if err != nil {
			return nil, err
		}
	}

	return opItem, nil
}

func (i *Parser) pushOperator(kind Kind) *Op {
	op := i.getOp()
	op.Kind = kind
	i.operatorStack = append(i.operatorStack, op)

	return op
}

func (i *Parser) parseStr(query string) (string, string, error) {
	if query[0] == '"' {
		return i.parseQuotedStr(query)
	}
	return i.parseRawStr(query)
}

func (i *Parser) parseRawStr(query string) (string, string, error) {
	k := 0
	passNext := false
	charBuf := make([]byte, 8)
	str := make([]byte, 0)
	for _, c := range query {
		if c == '\\' {
			passNext = true
			k += utf8.RuneLen(c)
			continue
		}

		if !passNext && !unicode.IsLetter(c) && !unicode.IsNumber(c) && c != '_' && c != '-' && c != '.' && c != '*' {
			break
		}

		n := utf8.EncodeRune(charBuf, c)
		k += n
		str = append(str, charBuf[:n]...)
		passNext = false
	}

	return query[k:], string(str), nil
}

func (i *Parser) parseQuotedStr(query string) (string, string, error) {
	if query[0] != '"' {
		return query, "", ParseErr{fmt.Errorf("unexpected start of quoted string")}
	}

	query = query[1:]
	r := make([]byte, 0)
	p := make([]byte, 8)
	passNext := false
	written := 0
	for _, c := range query {
		if !passNext {
			if c == '\\' {
				written++
				passNext = true
				continue
			}
			if c == '"' {
				written++
				break
			}
		}
		passNext = false

		l := utf8.EncodeRune(p, c)
		r = append(r, p[:l]...)
		written += l
	}

	return query[written:], util.ByteToStringUnsafe(r), nil
}

func (i *Parser) parseRange(query string) (string, string, error) {
	skip := false
	for i, c := range query {
		if skip {
			skip = false
			continue
		}
		if c == '\\' {
			skip = true
		}
		if c == ']' || c == '}' {
			return query[i+1:], query[:i+1], nil
		}
	}
	return query, "", fmt.Errorf("couldn't find closing bracket")
}

func (i *Parser) parseFieldQuery(query, field string) (string, error) {
	if field == "" {
		return query, ParseErr{fmt.Errorf("unexpected end of operand")}
	}

	if !i.tokenizer.IsFieldIndexed(field) {
		return query, fmt.Errorf("field '%s' is not indexed, try to search something else", field)
	}

	var fieldQ string
	var err error
	rangeType := query[0] == '[' || query[0] == '{'
	if rangeType {
		query, fieldQ, err = i.parseRange(query)
	} else {
		query, fieldQ, err = i.parseStr(query)
	}
	if err != nil {
		return "", err
	}

	i.state = stateOperator

	var tokens []Token

	if !rangeType && i.tokenizer.mapping[field].Main.TokenizerType == TokenizerTypeText {
		i.tokenizer.tokenBuf = []byte{}
		i.tokenizer.resultTokens = []Token{}
		i.tokenizer.TokenizeTextString(field, fieldQ, TextTypeIsSameTokenFunc)
		tokens = i.tokenizer.resultTokens
	} else if !rangeType && i.tokenizer.mapping[field].Main.TokenizerType == TokenizerTypeKeywordList {
		i.tokenizer.tokenBuf = []byte{}
		i.tokenizer.resultTokens = []Token{}
		i.tokenizer.TokenizeTextString(field, fieldQ, KeywordListTypeIsSameTokenFunc)
		tokens = i.tokenizer.resultTokens
	} else {
		tokens = append([]Token{}, Token{
			Field: append([]byte{}, field...),
			Val:   append([]byte{}, strings.ToLower(fieldQ)...),
		})
	}

	if len(tokens) == 0 {
		return query, nil
	}

	and := i.getOp()

	for _, token := range tokens {
		or := i.getOp()
		or.Kind = KindOr
		or.PosChilds = 1
		or.Index = len(i.plan.Ops)
		or.NextOp = and
		i.plan.Ops = append(i.plan.Ops, or)

		var kind Kind
		if rangeType {
			kind = KindSourceRange
		} else {
			kind = KindSourceMatch
		}
		source := i.addSourceOperand(token, kind)
		source.NextOp = or
	}

	and.Kind = KindAnd
	and.PosChilds = len(tokens)
	and.Index = len(i.plan.Ops)
	i.plan.Ops = append(i.plan.Ops, and)

	_, err = i.pushOperand(and)
	if err != nil {
		return query, err
	}
	return query, nil
}

func (i *Parser) parseOperand(query string) (string, error) {
	query, field, _ := i.parseRawStr(query)

	if field == "" {
		return query, fmt.Errorf("unexpected end of operand")
	}

	query = i.skipWC(query)
	if query == "" {
		return query, fmt.Errorf("unexpected end of operand")
	}

	if query[0] != ':' {
		return query, fmt.Errorf(`expected : after field name`)
	}
	query = query[1:]

	query = i.skipWC(query)

	if query == "" {
		return query, fmt.Errorf("unexpected end of operand")
	}

	var err error
	query, err = i.parseFieldQuery(query, field)
	if err != nil {
		return query, err
	}
	return query, nil
}
