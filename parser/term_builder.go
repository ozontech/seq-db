package parser

import (
	"strings"
)

func newTextTerm(text string) Term {
	return Term{
		Kind: TermText,
		Data: text,
	}
}

func newTextTermCaseSensitive(text string, caseSensitive bool) Term {
	if !caseSensitive {
		text = strings.ToLower(text)
	}
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
