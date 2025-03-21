package tokenizer

import (
	"bytes"
	"unicode"
	"unicode/utf8"

	"github.com/ozontech/seq-db/frac"
)

type Tokenizer interface {
	Tokenize(tokens []frac.MetaToken, key, value []byte, maxLength int) []frac.MetaToken
}

func toLowerIfCaseInsensitive(isCaseSensitive bool, x []byte) []byte {
	if isCaseSensitive {
		return x
	}

	return toLowerTryInplace(x)
}

// toLowerTryInplace tries to lowercase given []byte inplace (without allocations)
// but if width of upper and lower rune differs, fallbacks to bytes.Map which returns new []byte
func toLowerTryInplace(s []byte) []byte {
	for i := 0; i < len(s); {
		if isASCII[s[i]] {
			s[i] = toLowerMap[s[i]]
			i++
			continue
		}

		// please, refer to bytes.Map implementation for details
		upper, upperWid := utf8.DecodeRune(s[i:])
		lower := unicode.To(unicode.LowerCase, upper)

		// size of original rune is not equal to size of lower rune
		// sometimes is the case, please refer to https://github.com/ozontech/seq-db/pull/123#discussion_r1944557462
		if utf8.RuneLen(lower) != upperWid {
			// nolint:gocritic // suggested change to use bytes.ToLower is ignored because ToLower logic is rewritten
			return bytes.Map(unicode.ToLower, s)
		}
		utf8.EncodeRune(s[i:], lower)

		i += upperWid
	}
	return s
}

var (
	// toLowerMap maps upper ASCII symbols to lower. It is safe to use it on utf8 bytes (i > utf8.RuneSelf),
	// since ASCII symbol cannot be a part of other utf8 encoded symbol https://en.wikipedia.org/wiki/UTF-8#Description
	toLowerMap [256]byte

	// isASCII      returns true for given byte `b` if b < utf8.RuneSelf
	isASCII [256]bool

	// isUpperASCII returns true for given byte `b` if 'A' <= b && b <= 'Z'
	isUpperASCII [256]bool

	// isTextToken  returns true for given byte `b` if that byte should be parsed by tokenizer (for more information refer to initIsTextToken).
	//
	// 128 bytes is enough for them, but we use 256 to skip bound checks when we use isTextToken[byte(i)].
	isTextToken [256]bool
)

func init() {
	initUpperToLowerMap()
	initIsASCII()
	initIsUpperASCII()
	initIsTextToken()
}

func initUpperToLowerMap() {
	for i := 0; i < 256; i++ {
		toLowerMap[i] = byte(i)

		if 'A' <= i && i <= 'Z' {
			toLowerMap[i] += 'a' - 'A'
		}
	}
}

func initIsASCII() {
	for i := 0; i < utf8.RuneSelf; i++ {
		isASCII[i] = true
	}
}

func initIsUpperASCII() {
	for i := 'A'; i <= 'Z'; i++ {
		isUpperASCII[i] = true
	}
}

func initIsTextToken() {
	for i := 0; i < 256; i++ {
		// letters and digits
		if 'a' <= i && i <= 'z' || 'A' <= i && i <= 'Z' || '0' <= i && i <= '9' {
			isTextToken[i] = true
		}

		// other characters
		if i == '_' || i == '*' {
			isTextToken[i] = true
		}
	}
}
