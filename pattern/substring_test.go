package pattern

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrefixFunction(t *testing.T) {
	a := assert.New(t)
	var str *substring

	str = newSubstringPattern([]byte("aabaaab"))
	a.Equal([]int32{0, 1, 0, 1, 2, 2, 3}, str.prefFunc, "wrong prefix function")

	str = newSubstringPattern([]byte("abacaba"))
	a.Equal([]int32{0, 0, 1, 0, 1, 2, 3}, str.prefFunc, "wrong prefix function")

	str = newSubstringPattern([]byte("abracadabra"))
	a.Equal([]int32{0, 0, 0, 1, 0, 1, 0, 1, 2, 3, 4}, str.prefFunc, "wrong prefix function")

	str = newSubstringPattern([]byte("abacdadba"))
	a.Equal([]int32{0, 0, 1, 0, 0, 1, 0, 0, 1}, str.prefFunc, "wrong prefix function")

	str = newSubstringPattern([]byte("!@#\"123{}();'!@#"))
	a.Equal([]int32{1, 2, 3}, str.prefFunc[len(str.prefFunc)-3:], "wrong prefix function")

	str = newSubstringPattern([]byte("template#find template in templates text"))
	a.Equal(int32(8), str.prefFunc[21], "wrong prefix function")
	a.Equal(int32(8), str.prefFunc[33], "wrong prefix function")
	a.Equal(int32(0), str.prefFunc[34], "wrong prefix function")
}

func testSubstring(a *assert.Assertions, cnt int, substr, text string) {
	subs := newSubstringPattern([]byte(substr))
	str := []byte(text)
	total := 0
	for {
		to := findSubstring(str, subs)
		if to == -1 {
			break
		}
		total++
		a.Equal(string(subs.val), string(str[to-len(subs.val):to]), "substring doesn't match")
		str = str[to:]
	}
	a.Equal(cnt, total, "wrong total number of matches")
}

func testSequence(a *assert.Assertions, cnt int, substr []string, text string) {
	subs := make([]*substring, len(substr))
	for i, s := range substr {
		subs[i] = newSubstringPattern([]byte(s))
	}
	res := findSequence([]byte(text), subs)
	a.Equal(cnt, res, "wrong total number of matches")
}

func TestSubstring(t *testing.T) {
	a := assert.New(t)

	testSubstring(a, 2, "aba", "abacaba")
	testSubstring(a, 0, "abc", "abacaba")
	testSubstring(a, 1, "abacaba", "abacaba")
	testSubstring(a, 0, "abacaba", "aba")
	testSubstring(a, 0, "longtext", "a")
	testSubstring(a, 4, "a", "abacaba")
	testSubstring(a, 0, "d", "abacaba")
	testSubstring(a, 1, "aca", "abacaba")
	testSubstring(a, 3, "aab", "aabaaabaab")
	testSubstring(a, 2, "aa", "aaaaa")       // actually there are 4, but for our purposes we want this behaviour
	testSubstring(a, 1, "abaab", "abaabaab") // actually there are 2

	testSubstring(a, 1, "needle", "can you find a needle in a haystack?")
	testSubstring(a, 1, "haystack", "can you find a needle in a haystack?")
	testSubstring(a, 0, "elephant", "can you find a needle in a haystack?")

	testSubstring(a, 1, "@", "symbols@test")
	testSubstring(a, 1, "!1337#", "woah!1337#test")
}

func TestSequence(t *testing.T) {
	a := assert.New(t)

	testSequence(a, 2, []string{"abra", "ada"}, "abracadabra")
	testSequence(a, 2, []string{"aba", "aba"}, "abacaba")
	testSequence(a, 2, []string{"aba", "caba"}, "abacaba")
	testSequence(a, 1, []string{"abacaba"}, "abacaba")
	testSequence(a, 0, []string{"abacaba"}, "aba")
	testSequence(a, 1, []string{"aba"}, "abacaba")
	testSequence(a, 0, []string{"dad"}, "abacaba")
	testSequence(a, 1, []string{"aba", "dad"}, "abacaba")
	testSequence(a, 0, []string{"dad", "aba"}, "abacaba")

	testSequence(a, 2, []string{"needle", "haystack"}, "can you find a needle in a haystack?")
	testSequence(a, 2, []string{"k8s_pod", "_prod"}, "\"k8s_pod\":{\"main_prod\"}")

	testSequence(a, 2, []string{"!13", "37#"}, "woah!13@37#test")
}
