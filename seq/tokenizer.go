package seq

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
