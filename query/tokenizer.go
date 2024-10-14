package query

import (
	"strings"
	"unicode"
	"unicode/utf8"

	insaneJSON "github.com/vitkovskii/insane-json"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/util"
)

type TokenizerType int

const (
	TokenizerTypeNoop        TokenizerType = 0
	TokenizerTypeKeyword     TokenizerType = 1
	TokenizerTypeText        TokenizerType = 2
	TokenizerTypeObject      TokenizerType = 3
	TokenizerTypeTags        TokenizerType = 4
	TokenizerTypeKeywordList TokenizerType = 5
	TokenizerTypePath        TokenizerType = 6
	TokenizerTypeNested      TokenizerType = 7
)

var TokenTypesToNames = map[TokenizerType]string{
	TokenizerTypeNoop:        "noop",
	TokenizerTypeKeyword:     "keyword",
	TokenizerTypeText:        "text",
	TokenizerTypeObject:      "object",
	TokenizerTypeTags:        "tags",
	TokenizerTypeKeywordList: "keyword-list",
	TokenizerTypePath:        "path",
	TokenizerTypeNested:      "nested",
}

var NamesToTokenTypes = map[string]TokenizerType{}

func init() {
	for k, v := range TokenTypesToNames {
		NamesToTokenTypes[v] = k
	}
}

type Tokenizer struct {
	resultTokens    []Token
	tokenBuf        []byte
	runeBuf         []byte
	sb              *strings.Builder
	maxTokenSize    int
	isCaseSensitive bool
	mapping         Mapping
}

type Token struct {
	Field []byte
	Val   []byte
}

func (t Token) HR() string {
	return string(t.Field) + ":" + string(t.Val)
}

func NewTokenizer(maxTokenSize int, isCaseSensitive bool, mapping Mapping) *Tokenizer {
	if mapping == nil {
		panic("no mapping")
	}
	return &Tokenizer{
		resultTokens:    make([]Token, 0, 16),
		tokenBuf:        make([]byte, 0),
		runeBuf:         make([]byte, 8),
		maxTokenSize:    maxTokenSize,
		mapping:         mapping,
		isCaseSensitive: isCaseSensitive,
		sb:              &strings.Builder{},
	}
}

func (t *Tokenizer) Reset() {
	t.resultTokens = t.resultTokens[:0]
	t.tokenBuf = t.tokenBuf[:0]
	t.runeBuf = t.runeBuf[:8]
}

func getFieldString(field *insaneJSON.Node) string {
	if field.IsArray() || field.IsObject() {
		return field.EncodeToString()
	}
	return field.AsString()
}

func (t *Tokenizer) Do(root *insaneJSON.Root) []Token {
	t.resultTokens = t.resultTokens[:0]
	t.tokenBuf = t.tokenBuf[:0]

	t.TokenizeObject("", root.Node)
	t.resultTokens = append(t.resultTokens, AllToken)

	return t.resultTokens
}

func (t *Tokenizer) addFieldExistenceToken(fieldName string) {
	token := Token{}
	token.Field = ExistsTokenName

	pos := len(t.tokenBuf)
	t.tokenBuf = append(t.tokenBuf, fieldName...)
	token.Val = t.tokenBuf[pos:]

	t.resultTokens = append(t.resultTokens, token)
}

func (t *Tokenizer) TokenizeKeyword(fieldName string, field *insaneJSON.Node) {
	fieldValue := getFieldString(field)
	if len(fieldValue) > 1024 {
		metric.SkippedIndexesKeyword.Inc()
		metric.SkippedIndexesBytesKeyword.Add(float64(len(fieldValue)))
		return
	}
	token := Token{}

	pos := len(t.tokenBuf)
	t.tokenBuf = append(t.tokenBuf, fieldName...)
	token.Field = t.tokenBuf[pos:]

	pos = len(t.tokenBuf)
	t.tokenBuf = append(t.tokenBuf, t.toLowerIfCaseInsensitive(fieldValue)...)
	token.Val = t.tokenBuf[pos:]

	t.resultTokens = append(t.resultTokens, token)

	t.addFieldExistenceToken(fieldName)
}

func (t *Tokenizer) TokenizeText(fieldName string, field *insaneJSON.Node) {
	fieldValue := getFieldString(field)
	if len(fieldValue) > consts.MaxTextFieldValueLength {
		metric.SkippedIndexesText.Inc()
		metric.SkippedIndexesBytesText.Add(float64(len(fieldValue)))
		return
	}
	t.TokenizeTextString(fieldName, fieldValue, TextTypeIsSameTokenFunc)
	t.addFieldExistenceToken(fieldName)
}

func (t *Tokenizer) TokenizeKeywordList(fieldName string, field *insaneJSON.Node) {
	fieldValue := getFieldString(field)
	if len(fieldValue) > consts.MaxKeywordListFieldValueLength {
		return
	}

	t.TokenizeTextString(fieldName, fieldValue, KeywordListTypeIsSameTokenFunc)
	t.addFieldExistenceToken(fieldName)
}

// IsFieldIndexed returns whether this field is valid for parsing.
func (t *Tokenizer) IsFieldIndexed(token string) bool {
	if _, ok := t.mapping[token]; ok {
		return true
	}
	return token == util.ByteToStringUnsafe(AllTokenName) ||
		token == util.ByteToStringUnsafe(ExistsTokenName) ||
		token == util.ByteToStringUnsafe(IndexTokenName)
}

func TextTypeIsSameTokenFunc(c rune) bool {
	if unicode.IsLetter(c) || unicode.IsNumber(c) {
		return true
	}
	if c == '_' || c == '*' {
		return true
	}
	return false
}

func KeywordListTypeIsSameTokenFunc(c rune) bool {
	return c != ' '
}

func (t *Tokenizer) toLowerIfCaseInsensitive(x string) string {
	if !t.isCaseSensitive {
		return strings.ToLower(x)
	}
	return x
}

func (t *Tokenizer) TokenizeTextString(fieldName, fieldValue string, isSameTokenFunc func(c rune) bool) {
	k := 0
	for i, c := range fieldValue {
		sameToken := isSameTokenFunc(c)

		x := i - k
		if sameToken {
			continue
		}

		if x > 0 && x <= t.maxTokenSize {
			token := Token{}

			pos := len(t.tokenBuf)
			t.tokenBuf = append(t.tokenBuf, fieldName...)
			token.Field = t.tokenBuf[pos:]

			pos = len(t.tokenBuf)
			t.tokenBuf = append(t.tokenBuf, t.toLowerIfCaseInsensitive(fieldValue[k:i])...)
			token.Val = t.tokenBuf[pos:]

			t.resultTokens = append(t.resultTokens, token)
		}
		k = i
		if !sameToken {
			k++
		}
	}

	if k >= len(fieldValue) {
		return
	}

	token := Token{}

	pos := len(t.tokenBuf)
	t.tokenBuf = append(t.tokenBuf, fieldName...)
	token.Field = t.tokenBuf[pos:]

	pos = len(t.tokenBuf)
	for _, k := range fieldValue[k:] {
		if !t.isCaseSensitive {
			k = unicode.ToLower(k)
		}

		l := utf8.EncodeRune(t.runeBuf, k)
		t.tokenBuf = append(t.tokenBuf, t.runeBuf[:l]...)
	}
	token.Val = t.tokenBuf[pos:]

	t.resultTokens = append(t.resultTokens, token)
}

func (t *Tokenizer) TokenizeField(fieldName string, fieldValue *insaneJSON.Node) {
	tokenTypes := t.mapping[fieldName]
	switch tokenTypes.Main.TokenizerType {
	case TokenizerTypeKeyword:
		t.TokenizeKeyword(fieldName, fieldValue)
	case TokenizerTypeText:
		t.TokenizeText(fieldName, fieldValue)
	case TokenizerTypeObject:
		t.TokenizeObject(fieldName, fieldValue)
	case TokenizerTypeTags:
		t.TokenizeTags(fieldName, fieldValue)
	case TokenizerTypeKeywordList:
		t.TokenizeKeywordList(fieldName, fieldValue)
	case TokenizerTypePath:
		t.TokenizeKeyword(fieldName, fieldValue)
	case TokenizerTypeNoop:
	default:
		panic("unknown tokenizer")
	}
}

func (t *Tokenizer) TokenizeObject(name string, root *insaneJSON.Node) {
	for _, field := range root.AsFields() {
		fieldName := field.AsString()
		if name != "" {
			t.sb.Reset()
			t.sb.WriteString(name)
			t.sb.WriteByte('.')
			t.sb.WriteString(fieldName)
			fieldName = t.sb.String()
		}
		t.TokenizeField(fieldName, field.AsFieldValue())
	}
	if name != "" {
		t.addFieldExistenceToken(name)
	}
}

func (t *Tokenizer) TokenizeTags(fieldName string, field *insaneJSON.Node) {
	sb := &strings.Builder{}
	for _, k := range field.AsArray() {
		sb.Reset()
		sb.WriteString(fieldName)
		sb.WriteByte('.')
		sb.WriteString(k.Dig("key").AsString())
		t.TokenizeKeyword(sb.String(), k.Dig("value"))
	}
	t.addFieldExistenceToken(fieldName)
}
