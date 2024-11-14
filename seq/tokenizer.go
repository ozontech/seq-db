package seq

import (
	"strings"

	"github.com/ozontech/seq-db/util"
)

const (
	TokenAll    = "_all_"
	TokenExists = "_exists_"
	TokenIndex  = "_index"
)

var (
	ExistsTokenName = []byte(TokenExists)
	AllTokenName    = []byte(TokenAll)
)

type TokenizerType int

const (
	TokenizerTypeNoop    TokenizerType = 0
	TokenizerTypeKeyword TokenizerType = 1
	TokenizerTypeText    TokenizerType = 2
	TokenizerTypeObject  TokenizerType = 3
	TokenizerTypeTags    TokenizerType = 4
	TokenizerTypePath    TokenizerType = 6
	TokenizerTypeNested  TokenizerType = 7
	TokenizerTypeExists  TokenizerType = 8
)

var TokenTypesToNames = map[TokenizerType]string{
	TokenizerTypeNoop:    "noop",
	TokenizerTypeKeyword: "keyword",
	TokenizerTypeText:    "text",
	TokenizerTypeObject:  "object",
	TokenizerTypeTags:    "tags",
	TokenizerTypePath:    "path",
	TokenizerTypeNested:  "nested",
	TokenizerTypeExists:  "exists",
}

var NamesToTokenTypes = map[string]TokenizerType{}

func init() {
	for k, v := range TokenTypesToNames {
		NamesToTokenTypes[v] = k
	}
}

type Token struct {
	Field []byte
	Val   []byte
}

func Tokens(tokens ...string) []Token {
	r := make([]Token, 0)
	for _, tokenStr := range tokens {
		fieldPos := strings.IndexByte(tokenStr, ':')
		var t Token
		if fieldPos < 0 {
			t = Token{
				Field: util.StringToByteUnsafe(tokenStr),
				Val:   []byte("some_val")}
		} else {
			t = Token{
				Field: util.StringToByteUnsafe(tokenStr[:fieldPos]),
				Val:   util.StringToByteUnsafe(tokenStr[fieldPos+1:]),
			}
		}

		r = append(r, t)
	}

	return r
}
