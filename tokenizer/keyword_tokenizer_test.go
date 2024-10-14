package tokenizer

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/frac"
)

func newFracToken(k, v string) frac.MetaToken {
	return frac.MetaToken{Key: []byte(k), Value: []byte(v)}
}

func TestKeywordTokenizerEmptyValue(t *testing.T) {
	tokenizer := NewKeywordTokenizer(10, true, true)

	expected := []frac.MetaToken{newFracToken("message", "")}
	tokens := tokenizer.Tokenize([]frac.MetaToken{}, []byte("message"), []byte{}, 10)

	assert.Equal(t, expected, tokens)
}

func TestKeywordTokenizerSimple1(t *testing.T) {
	tokenizer := NewKeywordTokenizer(10, true, true)

	value := []byte("woRld")
	expected := []frac.MetaToken{newFracToken("message", "woRld")}
	tokens := tokenizer.Tokenize([]frac.MetaToken{}, []byte("message"), value, 10)

	assert.Equal(t, expected, tokens)
}

func TestKeywordTokenizerMaxLength(t *testing.T) {
	value := "hello world"

	// maxSize as argument
	tokenizer := NewKeywordTokenizer(100, true, false)
	tokens := tokenizer.Tokenize([]frac.MetaToken{}, []byte("message"), []byte(value), 10)
	assert.Equal(t, []frac.MetaToken{}, tokens)

	// default maxSize
	tokenizer = NewKeywordTokenizer(10, true, false)
	tokens = tokenizer.Tokenize([]frac.MetaToken{}, []byte("message"), []byte(value), 0)
	assert.Equal(t, []frac.MetaToken{}, tokens)
}

func TestKeywordTokenizerCaseSensitive(t *testing.T) {
	tokenizer := NewKeywordTokenizer(16, false, true)

	value := "heLlo WoRld"
	tokens := tokenizer.Tokenize([]frac.MetaToken{}, []byte("message"), []byte(value), 16)

	assert.Equal(t, []frac.MetaToken{newFracToken("message", "hello world")}, tokens)
}

func TestKeywordTokenizerPartialIndexing(t *testing.T) {
	const maxSize = 16
	value := "heLloWoRld1341341341324134134134123134134"

	// maxSize as argument
	tokenizer := NewKeywordTokenizer(100, true, true)

	tokens := tokenizer.Tokenize([]frac.MetaToken{}, []byte("message"), []byte(value), maxSize)
	assert.Equal(t, []frac.MetaToken{newFracToken("message", value[:maxSize])}, tokens)

	// default maxSize
	tokenizer = NewKeywordTokenizer(maxSize, true, true)
	tokens = tokenizer.Tokenize([]frac.MetaToken{}, []byte("message"), []byte(value), 0)
	assert.Equal(t, []frac.MetaToken{newFracToken("message", value[:maxSize])}, tokens)
}
