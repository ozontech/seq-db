package parser

import (
	"fmt"
	"unicode"
	"unicode/utf8"
)

func newTextTerm(text string) Term {
	return Term{
		Kind: TermText,
		Data: text,
	}
}

func newSymbolTerm(r rune) Term {
	return Term{
		Kind: TermSymbol,
		Data: string(r),
	}
}

type termBuilder interface {
	appendRune(rune) error
	appendWildcard() error
}

type tokenBuilder interface {
	termBuilder
	getTokens() []Token
}

type baseTokenBuilder struct {
	fieldName     string
	caseSensitive bool

	tokens []Token
	terms  []Term
	term   []byte
}

func (b *baseTokenBuilder) finishTextTerm() {
	if len(b.term) != 0 {
		b.terms = append(b.terms, newTextTerm(string(b.term)))
		b.term = b.term[:0]
	}
}

func (b *baseTokenBuilder) finishToken() {
	b.finishTextTerm()
	if len(b.terms) != 0 {
		b.tokens = append(b.tokens, &Literal{
			Field: b.fieldName,
			Terms: append(make([]Term, 0, len(b.terms)), b.terms...),
		})
		b.terms = b.terms[:0]
	}
}

func (b *baseTokenBuilder) getTokens() []Token {
	b.finishToken()
	return b.tokens
}

func (b *baseTokenBuilder) appendRuneInternal(r rune) {
	if !b.caseSensitive {
		r = unicode.ToLower(r)
	}
	b.term = utf8.AppendRune(b.term, r)
}

func (b *baseTokenBuilder) appendSymbolTerm(c rune) {
	b.finishTextTerm()
	b.terms = append(b.terms, newSymbolTerm(c))
}

func (b *baseTokenBuilder) endsWithSymbol(c byte) bool {
	if len(b.term) == 0 && len(b.terms) != 0 {
		lastTerm := b.terms[len(b.terms)-1]
		return lastTerm.Kind == TermSymbol && lastTerm.Data[0] == c
	}
	return false
}

type keywordTokenBuilder struct {
	baseTokenBuilder
}

func (b *keywordTokenBuilder) appendRune(r rune) error {
	b.appendRuneInternal(r)
	return nil
}

func (b *keywordTokenBuilder) appendWildcard() error {
	if b.endsWithSymbol('*') {
		return fmt.Errorf("duplicate wildcard symbol '*'")
	}
	b.appendSymbolTerm('*')
	return nil
}

type textTokenBuilder struct {
	baseTokenBuilder

	isIndexed func(rune) bool
}

func (b *textTokenBuilder) appendRune(r rune) error {
	if b.isIndexed(r) {
		b.appendRuneInternal(r)
	} else {
		b.finishToken()
	}
	return nil
}

func (b *textTokenBuilder) appendWildcard() error {
	if b.endsWithSymbol('*') {
		b.finishToken()
	}
	b.appendSymbolTerm('*')
	return nil
}

type singleTermBuilder struct {
	wildcard bool
	data     []byte
}

func (b *singleTermBuilder) appendRune(r rune) error {
	if b.wildcard {
		return fmt.Errorf("only single wildcard is allowed")
	}
	b.data = utf8.AppendRune(b.data, r)
	return nil
}

func (b *singleTermBuilder) appendWildcard() error {
	if b.wildcard || len(b.data) != 0 {
		return fmt.Errorf("only single wildcard is allowed")
	}
	b.wildcard = true
	return nil
}

func (b *singleTermBuilder) getTerm() Term {
	if b.wildcard {
		return newSymbolTerm('*')
	}
	return newTextTerm(string(b.data))
}
