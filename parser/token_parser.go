package parser

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/ozontech/seq-db/seq"

	"github.com/ozontech/seq-db/conf"
)

type tokenParser struct {
	data []rune
	pos  int
}

func (tp *tokenParser) errorEOF(expected string, args ...any) error {
	return fmt.Errorf("unexpected end of query, expected %s", fmt.Sprintf(expected, args...))
}

func (tp *tokenParser) errorWrap(err error) error {
	return fmt.Errorf(`%s at pos %d`, err.Error(), tp.pos)
}

func (tp *tokenParser) errorUnexpected(pos int, what string, args ...any) error {
	return fmt.Errorf(`unexpected %s at pos %d`, fmt.Sprintf(what, args...), pos)
}

func (tp *tokenParser) errorUnexpectedSymbol(where string, args ...any) error {
	pos := tp.pos
	word := tp.parseSimpleTerm()
	tp.pos = pos
	if word != "" {
		return fmt.Errorf(`unexpected term "%s" %s at pos %d`, word, fmt.Sprintf(where, args...), tp.pos)
	}
	return fmt.Errorf(`unexpected symbol '%c' %s at pos %d`, tp.cur(), fmt.Sprintf(where, args...), tp.pos)
}

func (tp *tokenParser) cur() rune {
	return tp.data[tp.pos]
}

func (tp *tokenParser) eof() bool {
	return tp.pos == len(tp.data)
}

func (tp *tokenParser) space() bool {
	return unicode.IsSpace(tp.cur())
}

func (tp *tokenParser) specialSymbol() bool {
	return specialSymbol[tp.cur()]
}

func (tp *tokenParser) graylogEscapedSymbol() bool {
	return graylogEscapedSymbol[tp.cur()]
}

func (tp *tokenParser) quoteEscapedSymbol() bool {
	return quoteEscapedSymbol[tp.cur()]
}

// skipSpaces fast forwards zero or more spaces
// most functions (with except for highest-level) expect no spaces when they kick in
// and always skip spaces after them
func (tp *tokenParser) skipSpaces() {
	for !tp.eof() && tp.space() {
		tp.pos++
	}
}

// parseSimpleTerm parses simple words, like field name or operators
func (tp *tokenParser) parseSimpleTerm() string {
	start := tp.pos
	for !tp.eof() && !tp.space() && !tp.specialSymbol() {
		tp.pos++
	}
	finish := tp.pos
	tp.skipSpaces()
	return string(tp.data[start:finish])
}

func (tp *tokenParser) parseTerms(tb termBuilder) error {
	for ; !tp.eof(); tp.pos++ {
		if tp.cur() == '*' {
			if err := tb.appendWildcard(); err != nil {
				return tp.errorWrap(err)
			}
			continue
		}
		if tp.cur() == '\\' {
			tp.pos++
			if tp.eof() {
				return tp.errorEOF(`escaped symbol`)
			}
			if !tp.space() && !tp.specialSymbol() && !tp.graylogEscapedSymbol() {
				return tp.errorUnexpectedSymbol("after '\\'")
			}
		} else if tp.space() || tp.specialSymbol() {
			break
		}
		if err := tb.appendRune(tp.cur()); err != nil {
			return tp.errorWrap(err)
		}
	}
	tp.skipSpaces()
	return nil
}

func (tp *tokenParser) parseQuotedTerms(tb termBuilder) error {
	if tp.cur() != '"' {
		panic("quote not found")
	}
	tp.pos++
	for ; !tp.eof(); tp.pos++ {
		switch tp.cur() {
		case '\\':
			tp.pos++
			if tp.eof() {
				return tp.errorEOF(`escaped symbol and closing quote '"'`)
			}
			if !tp.quoteEscapedSymbol() {
				if err := tb.appendRune('\\'); err != nil {
					return tp.errorWrap(err)
				}
			}
			if err := tb.appendRune(tp.cur()); err != nil {
				return tp.errorWrap(err)
			}
		case '*':
			if err := tb.appendWildcard(); err != nil {
				return tp.errorWrap(err)
			}
		case '"':
			tp.pos++
			tp.skipSpaces()
			return nil
		default:
			if err := tb.appendRune(tp.cur()); err != nil {
				return tp.errorWrap(err)
			}
		}
	}
	return tp.errorEOF(`closing quote '"'`)
}

func (tp *tokenParser) parseRangeTerm(term *Term) error {
	builder := singleTermBuilder{}
	var err error
	var quoted bool
	if !tp.eof() && tp.cur() == '"' {
		quoted = true
		err = tp.parseQuotedTerms(&builder)
	} else {
		err = tp.parseTerms(&builder)
	}
	if err != nil {
		return err
	}
	*term = builder.getTerm()
	if term.Data == "" && !quoted {
		if tp.eof() {
			return tp.errorEOF("range bounding term")
		}
		return tp.errorUnexpectedSymbol(`instead of range bounding term`)
	}
	return nil
}

func (tp *tokenParser) parseRange(r *Range) error {
	switch tp.cur() {
	case '[':
		r.IncludeFrom = true
	case '{':
		r.IncludeFrom = false
	default:
		panic("range start not found")
	}
	tp.pos++
	tp.skipSpaces()
	if err := tp.parseRangeTerm(&r.From); err != nil {
		return err
	}
	toPos := tp.pos
	to := tp.parseSimpleTerm()
	if !strings.EqualFold(to, "to") {
		if tp.eof() {
			return tp.errorEOF(`"to" keyword`)
		}
		if to == "" {
			tp.pos = toPos
			return tp.errorUnexpectedSymbol("instead of \"to\" keyword in range")
		}
		return tp.errorUnexpected(toPos, `term "%s" instead of "to" keyword in range`, to)
	}
	if err := tp.parseRangeTerm(&r.To); err != nil {
		return err
	}
	if tp.eof() {
		return tp.errorEOF("closing bracket (either ']' or '}') of range")
	}
	switch tp.cur() {
	case ']':
		r.IncludeTo = true
	case '}':
		r.IncludeTo = false
	default:
		return tp.errorUnexpectedSymbol(`in place of range closing bracket (either ']' or '}')`)
	}
	tp.pos++
	tp.skipSpaces()
	return nil
}

func (tp *tokenParser) parseLiteral(fieldName string, indexType seq.TokenizerType) ([]Token, error) {
	caseSensitive := conf.CaseSensitive
	if fieldName == seq.TokenExists {
		caseSensitive = true
	}

	if tp.eof() {
		return nil, tp.errorEOF("search term")
	}
	if tp.cur() == '[' || tp.cur() == '{' {
		r := &Range{Field: fieldName}
		if err := tp.parseRange(r); err != nil {
			return nil, err
		}
		return []Token{r}, nil
	}
	var lb tokenBuilder
	baseBuilder := baseTokenBuilder{
		fieldName:     fieldName,
		caseSensitive: caseSensitive,
	}
	switch indexType {
	case seq.TokenizerTypeText:
		lb = &textTokenBuilder{
			baseTokenBuilder: baseBuilder,
			isIndexed: func(c rune) bool {
				if unicode.IsLetter(c) || unicode.IsNumber(c) {
					return true
				}
				if c == '_' || c == '*' {
					return true
				}
				return false
			},
		}
	case seq.TokenizerTypeKeyword, seq.TokenizerTypePath:
		lb = &keywordTokenBuilder{
			baseTokenBuilder: baseBuilder,
		}
	default:
		panic("unknown index type")
	}
	pos := tp.pos
	if tp.cur() == '"' {
		if err := tp.parseQuotedTerms(lb); err != nil {
			return nil, err
		}
		tokens := lb.getTokens()
		if len(tokens) == 0 {
			return []Token{&Literal{
				Field: fieldName,
				Terms: []Term{{
					Kind: TermText,
					Data: "",
				}},
			}}, nil
		}
		return tokens, nil
	}
	if err := tp.parseTerms(lb); err != nil {
		return nil, err
	}
	tokens := lb.getTokens()
	if len(tokens) == 0 {
		if pos == tp.pos {
			return nil, tp.errorUnexpectedSymbol("instead of search term")
		}
		return nil, tp.errorUnexpected(pos, `sequence "%s" instead of token query term`, string(tp.data[pos:tp.pos]))
	}
	return tokens, nil
}

func (tp *tokenParser) parseTokenQuery(fieldName string, indexType seq.TokenizerType) ([]Token, error) {
	if tp.eof() {
		return nil, tp.errorEOF(`field name separator ':'`)
	}
	if tp.cur() != ':' {
		return nil, tp.errorUnexpectedSymbol(`instead of field name separator ':' after "%s"`, fieldName)
	}
	tp.pos++
	tp.skipSpaces()
	return tp.parseLiteral(fieldName, indexType)
}
