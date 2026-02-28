package tokenizer

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

const maxTokenSizeDummy = 0

var longDocument = []byte("/T1.T2_T3,t4.looooong_t5/readyz error* 5555-r2")

func TestTokenizeEmptyValue(t *testing.T) {
	testCase := []byte("")
	tokenizer := NewTextTokenizer(1000, false, true, 1024)

	tokens := tokenizer.Tokenize([]MetaToken{}, []byte("message"), testCase, maxTokenSizeDummy)
	expected := []MetaToken{newMetaToken("message", "")}

	assert.Equal(t, expected, tokens)
}

func TestTokenizeSimple(t *testing.T) {
	testCase := []byte("arr hello world")
	tokenizer := NewTextTokenizer(1000, false, true, 1024)

	tokens := tokenizer.Tokenize(nil, []byte("message"), testCase, maxTokenSizeDummy)
	assert.Equal(t, newMetaToken("message", "arr"), tokens[0])
	assert.Equal(t, newMetaToken("message", "hello"), tokens[1])
	assert.Equal(t, newMetaToken("message", "world"), tokens[2])
}

func TestTokenizeSimple2(t *testing.T) {
	tokenizer := NewTextTokenizer(1000, false, true, 1024)
	tokens := tokenizer.Tokenize(nil, []byte("message"), bytes.Clone(longDocument), maxTokenSizeDummy)

	assert.Equal(t, newMetaToken("message", "t1"), tokens[0])
	assert.Equal(t, newMetaToken("message", "t2_t3"), tokens[1])
	assert.Equal(t, newMetaToken("message", "t4"), tokens[2])
	assert.Equal(t, newMetaToken("message", "looooong_t5"), tokens[3])
	assert.Equal(t, newMetaToken("message", "readyz"), tokens[4])
	assert.Equal(t, newMetaToken("message", "error*"), tokens[5])
	assert.Equal(t, newMetaToken("message", "5555"), tokens[6])
	assert.Equal(t, newMetaToken("message", "r2"), tokens[7])
}

func TestTokenizePartialDefault(t *testing.T) {
	const maxSize = 100500
	tokenizer := NewTextTokenizer(maxSize, false, true, maxSize)
	testCase := []byte(strings.Repeat("1", maxSize+1))

	tokens := tokenizer.Tokenize([]MetaToken{}, []byte("message"), testCase, maxTokenSizeDummy)

	expected := []MetaToken{newMetaToken("message", strings.Repeat("1", maxSize))}

	assert.Equal(t, expected, tokens)
}

func TestTokenizePartial(t *testing.T) {
	const maxSize = 100500
	tokenizer := NewTextTokenizer(maxSize, false, true, 0)
	testCase := []byte(strings.Repeat("1", maxSize+1))

	tokens := tokenizer.Tokenize(nil, []byte("message"), testCase, maxSize)

	expected := []MetaToken{newMetaToken("message", strings.Repeat("1", maxSize))}

	assert.Equal(t, expected, tokens)
}

func TestTokenizePartialSkipDefault(t *testing.T) {
	const maxSize = 100500
	tokenizer := NewTextTokenizer(maxSize, false, false, maxSize)
	testCase := []byte(strings.Repeat("1", maxSize+1))

	tokens := tokenizer.Tokenize([]MetaToken{}, []byte("message"), testCase, maxTokenSizeDummy)

	assert.Equal(t, []MetaToken{}, tokens)
}

func TestTokenizePartialSkip(t *testing.T) {
	const maxSize = 100500
	tokenizer := NewTextTokenizer(maxSize, false, false, 0)
	testCase := []byte(strings.Repeat("1", maxSize+1))

	tokens := tokenizer.Tokenize([]MetaToken{}, []byte("message"), testCase, maxSize)

	assert.Equal(t, []MetaToken{}, tokens)
}

func TestTokenizeDefaultMaxTokenSize(t *testing.T) {
	tokenizer := NewTextTokenizer(6, false, true, 1024)
	tokens := tokenizer.Tokenize(nil, []byte("message"), bytes.Clone(longDocument), maxTokenSizeDummy)

	assert.Equal(t, newMetaToken("message", "t1"), tokens[0])
	assert.Equal(t, newMetaToken("message", "t2_t3"), tokens[1])
	assert.Equal(t, newMetaToken("message", "t4"), tokens[2])
	assert.Equal(t, newMetaToken("message", "readyz"), tokens[3])
	assert.Equal(t, newMetaToken("message", "error*"), tokens[4])
	assert.Equal(t, newMetaToken("message", "5555"), tokens[5])
	assert.Equal(t, newMetaToken("message", "r2"), tokens[6])
}

func TestTokenizeCaseSensitive(t *testing.T) {
	tokenizer := NewTextTokenizer(1000, true, true, 1024)

	tokens := tokenizer.Tokenize(nil, []byte("message"), bytes.Clone(longDocument), maxTokenSizeDummy)

	assert.Equal(t, newMetaToken("message", "T1"), tokens[0])
	assert.Equal(t, newMetaToken("message", "T2_T3"), tokens[1])
	assert.Equal(t, newMetaToken("message", "t4"), tokens[2])
	assert.Equal(t, newMetaToken("message", "looooong_t5"), tokens[3])
	assert.Equal(t, newMetaToken("message", "readyz"), tokens[4])
	assert.Equal(t, newMetaToken("message", "error*"), tokens[5])
	assert.Equal(t, newMetaToken("message", "5555"), tokens[6])
	assert.Equal(t, newMetaToken("message", "r2"), tokens[7])
}

func TestTokenizeCaseSensitiveAndMaxTokenSize(t *testing.T) {
	tokenizer := NewTextTokenizer(6, true, true, 1024)

	tokens := tokenizer.Tokenize(nil, []byte("message"), bytes.Clone(longDocument), maxTokenSizeDummy)

	assert.Equal(t, newMetaToken("message", "T1"), tokens[0])
	assert.Equal(t, newMetaToken("message", "T2_T3"), tokens[1])
	assert.Equal(t, newMetaToken("message", "t4"), tokens[2])
	assert.Equal(t, newMetaToken("message", "readyz"), tokens[3])
	assert.Equal(t, newMetaToken("message", "error*"), tokens[4])
	assert.Equal(t, newMetaToken("message", "5555"), tokens[5])
	assert.Equal(t, newMetaToken("message", "r2"), tokens[6])
}

func TestTokenizeLastTokenLength(t *testing.T) {
	testCase := []byte("1 10")
	tokenizer := NewTextTokenizer(1, true, true, 1024)

	tokens := tokenizer.Tokenize(nil, []byte("message"), testCase, maxTokenSizeDummy)
	assert.Equal(t, 1, len(tokens))
	assert.Equal(t, newMetaToken("message", "1"), tokens[0])
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

			tokens := tokenizer.Tokenize([]MetaToken{}, []byte("message"), []byte(in), maxTokenSizeDummy)

			expected := []MetaToken{}
			for _, token := range out {
				if lowercase {
					token = strings.ToLower(token)
				}
				expected = append(expected, newMetaToken("message", token))
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

func BenchmarkTokenize(b *testing.B) {
	tokenizer := NewTextTokenizer(1000, false, true, 1024)
	name := []byte("message")

	short := []byte("GET /api/v1/users 200 OK")
	medium := []byte("2025-02-27T10:15:30Z INFO worker_3 processed request from 192.168.1.42 method=POST path=/api/v1/orders status=201 latency_ms=12 bytes=4096")
	long := bytes.Repeat([]byte("connection_timeout from host=server42 region=eu_west error_code=ETIMEDOUT retry_count=3 "), 10)

	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"short_24B", short},
		{"medium_150B", medium},
		{"long_900B", long},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(tc.data)))
			var tokens []MetaToken
			for b.Loop() {
				tokens = tokenizer.Tokenize(tokens[:0], name, tc.data, 0)
			}
		})
	}
}
