package pattern

import (
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testFindSequence(a *assert.Assertions, cnt int, needles []string, haystack string) {
	var needlesB [][]byte
	for _, needle := range needles {
		needlesB = append(needlesB, []byte(needle))
	}
	res := findSequence([]byte(haystack), needlesB)
	a.Equal(cnt, res, "wrong total number of matches")
}

func TestFindSequence(t *testing.T) {
	a := assert.New(t)

	testFindSequence(a, 2, []string{"abra", "ada"}, "abracadabra")
	testFindSequence(a, 2, []string{"aba", "aba"}, "abacaba")
	testFindSequence(a, 2, []string{"aba", "caba"}, "abacaba")
	testFindSequence(a, 1, []string{"abacaba"}, "abacaba")
	testFindSequence(a, 0, []string{"abacaba"}, "aba")
	testFindSequence(a, 1, []string{"aba"}, "abacaba")
	testFindSequence(a, 0, []string{"dad"}, "abacaba")
	testFindSequence(a, 1, []string{"aba", "dad"}, "abacaba")
	testFindSequence(a, 0, []string{"dad", "aba"}, "abacaba")

	testFindSequence(a, 2, []string{"needle", "haystack"}, "can you find a needle in a haystack?")
	testFindSequence(a, 2, []string{"k8s_pod", "_prod"}, "\"k8s_pod\":{\"main_prod\"}")

	testFindSequence(a, 2, []string{"!13", "37#"}, "woah!13@37#test")

	testFindSequence(a, 1, []string{"abc"}, strings.Repeat("ab", 1024)+"c")
}

func BenchmarkFindSequence_Deterministic(b *testing.B) {
	type testCase struct {
		haystack []byte
		needles  [][]byte
	}

	type namedTestCase struct {
		name  string
		cases []testCase
	}

	testCases := []namedTestCase{
		{
			name: "regular-cases",
			cases: []testCase{
				{bb("Hello, world!"), [][]byte{bb("orl")}},
				{bb("some-k8s-service"), [][]byte{bb("k8s")}},
			},
		},
		{
			name: "corner-cases",
			cases: []testCase{
				{bb(strings.Repeat("ab", 32) + "c"), [][]byte{bb("abc")}},
				{bb(strings.Repeat("ab", 64) + "c"), [][]byte{bb("abc")}},
				{bb(strings.Repeat("ab", 1024) + "c"), [][]byte{bb("abc")}},
				{bb(strings.Repeat("ab", 16384) + "c"), [][]byte{bb("abc")}},
			},
		},
	}

	for _, tc := range testCases {
		for i, c := range tc.cases {
			b.Run(tc.name+"-"+strconv.Itoa(i), func(b *testing.B) {
				for b.Loop() {
					findSequence([]byte(c.haystack), c.needles)
				}
			})
		}
	}
}

func BenchmarkFindSequence_Random(b *testing.B) {
	sizes := []struct {
		name         string
		haystackSize int
		needleSize   int
		needleCount  int
	}{
		{"tiny", 64, 3, 2},
		{"small", 256, 10, 3},
		{"medium", 1024, 50, 5},
		{"large", 16384, 200, 10},
		{"extra-large", 1048576, 1024, 100},
	}

	for _, size := range sizes {
		b.Run(size.name, func(b *testing.B) {
			haystack, needles := generateTestData(
				size.haystackSize, size.needleSize, size.needleCount, 256,
			)
			b.ResetTimer()
			for b.Loop() {
				findSequence(haystack, needles)
				b.SetBytes(int64(len(haystack)))
			}
		})
	}
}

func generateTestData(haystackSize, needleSize, needleCount, charset int) ([]byte, [][]byte) {
	haystack := generateRandomBytes(haystackSize, charset)

	needles := make([][]byte, needleCount)
	for i := range needleCount {
		pattern := generateRandomBytes(needleSize, charset)
		pos := rand.Intn(len(haystack) - needleSize)
		copy(haystack[pos:], pattern)
		needles[i] = pattern
	}

	return haystack, needles
}

func generateRandomBytes(size, charset int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(rand.Intn(charset))
	}
	return b
}

func bb(s string) []byte {
	return []byte(s)
}
