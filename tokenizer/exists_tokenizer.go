package tokenizer

type ExistsTokenizer struct{}

func NewExistsTokenizer() *ExistsTokenizer {
	return &ExistsTokenizer{}
}

func (t *ExistsTokenizer) Tokenize(tokens []MetaToken, _, _ []byte, _ int) []MetaToken {
	return tokens
}
