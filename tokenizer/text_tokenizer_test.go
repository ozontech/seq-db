package tokenizer

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/frac"
)

const maxTokenSizeDummy = 0

var longDocument = []byte("/T1.T2_T3,t4.looooong_t5/readyz error* 5555-r2")

func TestTokenizeEmptyValue(t *testing.T) {
	testCase := []byte("")
	tokenizer := NewTextTokenizer(1000, false, true, 1024)

	tokens := tokenizer.Tokenize([]frac.MetaToken{}, []byte("message"), testCase, maxTokenSizeDummy)
	expected := []frac.MetaToken{newFracToken("message", "")}

	assert.Equal(t, expected, tokens)
}

func TestTokenizeSimple(t *testing.T) {
	testCase := []byte("arr hello world")
	tokenizer := NewTextTokenizer(1000, false, true, 1024)

	tokens := tokenizer.Tokenize(nil, []byte("message"), testCase, maxTokenSizeDummy)
	assert.Equal(t, newFracToken("message", "arr"), tokens[0])
	assert.Equal(t, newFracToken("message", "hello"), tokens[1])
	assert.Equal(t, newFracToken("message", "world"), tokens[2])
}

func TestTokenizeSimple2(t *testing.T) {
	tokenizer := NewTextTokenizer(1000, false, true, 1024)
	tokens := tokenizer.Tokenize(nil, []byte("message"), longDocument, maxTokenSizeDummy)

	assert.Equal(t, newFracToken("message", "t1"), tokens[0])
	assert.Equal(t, newFracToken("message", "t2_t3"), tokens[1])
	assert.Equal(t, newFracToken("message", "t4"), tokens[2])
	assert.Equal(t, newFracToken("message", "looooong_t5"), tokens[3])
	assert.Equal(t, newFracToken("message", "readyz"), tokens[4])
	assert.Equal(t, newFracToken("message", "error*"), tokens[5])
	assert.Equal(t, newFracToken("message", "5555"), tokens[6])
	assert.Equal(t, newFracToken("message", "r2"), tokens[7])
}

func TestTokenizePartialDefault(t *testing.T) {
	const maxSize = 100500
	tokenizer := NewTextTokenizer(maxSize, false, true, maxSize)
	testCase := []byte(strings.Repeat("1", maxSize+1))

	tokens := tokenizer.Tokenize([]frac.MetaToken{}, []byte("message"), testCase, maxTokenSizeDummy)

	expected := []frac.MetaToken{newFracToken("message", strings.Repeat("1", maxSize))}

	assert.Equal(t, expected, tokens)
}

func TestTokenizePartial(t *testing.T) {
	const maxSize = 100500
	tokenizer := NewTextTokenizer(maxSize, false, true, 0)
	testCase := []byte(strings.Repeat("1", maxSize+1))

	tokens := tokenizer.Tokenize(nil, []byte("message"), testCase, maxSize)

	expected := []frac.MetaToken{newFracToken("message", strings.Repeat("1", maxSize))}

	assert.Equal(t, expected, tokens)
}

func TestTokenizePartialSkipDefault(t *testing.T) {
	const maxSize = 100500
	tokenizer := NewTextTokenizer(maxSize, false, false, maxSize)
	testCase := []byte(strings.Repeat("1", maxSize+1))

	tokens := tokenizer.Tokenize([]frac.MetaToken{}, []byte("message"), testCase, maxTokenSizeDummy)

	assert.Equal(t, []frac.MetaToken{}, tokens)
}

func TestTokenizePartialSkip(t *testing.T) {
	const maxSize = 100500
	tokenizer := NewTextTokenizer(maxSize, false, false, 0)
	testCase := []byte(strings.Repeat("1", maxSize+1))

	tokens := tokenizer.Tokenize([]frac.MetaToken{}, []byte("message"), testCase, maxSize)

	assert.Equal(t, []frac.MetaToken{}, tokens)
}

func TestTokenizeDefaultMaxTokenSize(t *testing.T) {
	tokenizer := NewTextTokenizer(6, false, true, 1024)
	tokens := tokenizer.Tokenize(nil, []byte("message"), longDocument, maxTokenSizeDummy)

	assert.Equal(t, newFracToken("message", "t1"), tokens[0])
	assert.Equal(t, newFracToken("message", "t2_t3"), tokens[1])
	assert.Equal(t, newFracToken("message", "t4"), tokens[2])
	assert.Equal(t, newFracToken("message", "readyz"), tokens[3])
	assert.Equal(t, newFracToken("message", "error*"), tokens[4])
	assert.Equal(t, newFracToken("message", "5555"), tokens[5])
	assert.Equal(t, newFracToken("message", "r2"), tokens[6])
}

func TestTokenizeCaseSensitive(t *testing.T) {
	tokenizer := NewTextTokenizer(1000, true, true, 1024)

	tokens := tokenizer.Tokenize(nil, []byte("message"), longDocument, maxTokenSizeDummy)

	assert.Equal(t, newFracToken("message", "T1"), tokens[0])
	assert.Equal(t, newFracToken("message", "T2_T3"), tokens[1])
	assert.Equal(t, newFracToken("message", "t4"), tokens[2])
	assert.Equal(t, newFracToken("message", "looooong_t5"), tokens[3])
	assert.Equal(t, newFracToken("message", "readyz"), tokens[4])
	assert.Equal(t, newFracToken("message", "error*"), tokens[5])
	assert.Equal(t, newFracToken("message", "5555"), tokens[6])
	assert.Equal(t, newFracToken("message", "r2"), tokens[7])
}

func TestTokenizeCaseSensitiveAndMaxTokenSize(t *testing.T) {
	tokenizer := NewTextTokenizer(6, true, true, 1024)

	tokens := tokenizer.Tokenize(nil, []byte("message"), longDocument, maxTokenSizeDummy)

	assert.Equal(t, newFracToken("message", "T1"), tokens[0])
	assert.Equal(t, newFracToken("message", "T2_T3"), tokens[1])
	assert.Equal(t, newFracToken("message", "t4"), tokens[2])
	assert.Equal(t, newFracToken("message", "readyz"), tokens[3])
	assert.Equal(t, newFracToken("message", "error*"), tokens[4])
	assert.Equal(t, newFracToken("message", "5555"), tokens[5])
	assert.Equal(t, newFracToken("message", "r2"), tokens[6])
}

func TestTokenizeLastTokenLength(t *testing.T) {
	testCase := []byte("1 10")
	tokenizer := NewTextTokenizer(1, true, true, 1024)

	tokens := tokenizer.Tokenize(nil, []byte("message"), testCase, maxTokenSizeDummy)
	assert.Equal(t, 1, len(tokens))
	assert.Equal(t, newFracToken("message", "1"), tokens[0])
}

func TestKeywordListTokenizerEmptyValue(t *testing.T) {
	tokenizer := NewKeywordListTokenizer(10, true, true, 1024)

	value := ""
	expected := []frac.MetaToken{newFracToken("message", "")}
	tokens := tokenizer.Tokenize(nil, []byte("message"), []byte(value), maxTokenSizeDummy)

	assert.Equal(t, expected, tokens)
}

func TestKeywordListTokenizerSimple1(t *testing.T) {
	tokenizer := NewKeywordListTokenizer(10, true, true, 1024)

	value := "hello world"
	expected := []frac.MetaToken{newFracToken("message", "hello"), newFracToken("message", "world")}
	tokens := tokenizer.Tokenize(nil, []byte("message"), []byte(value), maxTokenSizeDummy)

	assert.Equal(t, expected, tokens)
}

func TestKeywordListTokenizerSimple2(t *testing.T) {
	tokenizer := NewKeywordListTokenizer(10, true, true, 1024)

	value := "hello woRLd this-is-a-long-token _okay_"
	expected := []frac.MetaToken{newFracToken("message", "hello"), newFracToken("message", "woRLd"), newFracToken("message", "_okay_")}
	tokens := tokenizer.Tokenize(nil, []byte("message"), []byte(value), maxTokenSizeDummy)

	assert.Equal(t, expected, tokens)
}

func TestKeywordListTokenizerCaseInsensitive(t *testing.T) {
	tokenizer := NewKeywordListTokenizer(10, false, true, 1024)

	value := "hello woRLd this-is-a-long-token _okay_"
	expected := []frac.MetaToken{newFracToken("message", "hello"), newFracToken("message", "world"), newFracToken("message", "_okay_")}
	tokens := tokenizer.Tokenize(nil, []byte("message"), []byte(value), maxTokenSizeDummy)

	assert.Equal(t, expected, tokens)
}

func TestTextTokenizerUTF8(t *testing.T) {
	test := func(s string, out []string) {
		t.Helper()

		for _, lowercase := range []bool{false, true} {
			in := s
			if lowercase {
				in = strings.ToLower(s)
			}

			tokenizer := NewTextTokenizer(100, true, true, 1024)

			tokens := tokenizer.Tokenize([]frac.MetaToken{}, []byte("message"), []byte(in), maxTokenSizeDummy)

			expected := []frac.MetaToken{}
			for _, token := range out {
				if lowercase {
					token = strings.ToLower(token)
				}
				expected = append(expected, newFracToken("message", token))
			}
			assert.Equal(t, expected, tokens)
		}
	}

	// Test 1 byte UTF8.
	test("hello world!", []string{"hello", "world"})
	// Test 2-byte UTF8.
	test("Привет Мир!", []string{"Привет", "Мир"})
	// Test tail flush.
	test("мир", []string{"мир"})
	// Test 3-byte UTF8.
	test("Hello, 世界!", []string{"Hello", "世界"})
	// Test 4-byte UTF8.
	test("🚀 world", []string{"world"})

	// Handle non-ASCII space characters.
	test("🚀🚀🚀", []string{})
	test("🚀 🚀 🚀", []string{})
	test("🚀SpaceX", []string{"SpaceX"})
	test("SpaceY🚀", []string{"SpaceY"})
	test("От🚀Земли🌏до🌚луны", []string{"От", "Земли", "до", "луны"})
	// Text mix of ASCII and non-ASCII characters.
	test("пРивеt世界", []string{"пРивеt世界"})
	test("А", []string{"А"})
}
