package tokenizer

import "github.com/ozontech/seq-db/frac"

type ExistsTokenizer struct{}

func NewExistsTokenizer() *ExistsTokenizer {
	return &ExistsTokenizer{}
}

func (t *ExistsTokenizer) Tokenize(tokens []frac.MetaToken, _, _ []byte, _ int) []frac.MetaToken {
	return tokens
}
