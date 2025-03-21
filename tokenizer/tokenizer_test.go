package tokenizer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToLowerTryInplace(t *testing.T) {
	t.Parallel()
	test := func(input, expected string) {
		t.Helper()
		// arrange
		inp := []byte(input)

		// act
		out := toLowerTryInplace(inp)

		// assert
		assert.Equal(t, []byte(expected), out)
	}

	test("lower ascii", "lower ascii")
	test("lower ascii with numbers 0-9", "lower ascii with numbers 0-9")
	test("UPPER ASCII", "upper ascii")
	test("UpPeR aNd LoWeR ASCII", "upper and lower ascii")

	test("lower юникод", "lower юникод")
	test("UPPER ЮНИКОД", "upper юникод")
	test("UpPeR aNd LoWeR ЮнИкОд", "upper and lower юникод")
	test("No lower letters  こんにちは。", "no lower letters  こんにちは。")
	test("Smileys 😉😉!!!", "smileys 😉😉!!!")
	test("Wider lower Ɒ", "wider lower ɒ")
	test("Narrower lower Ⱦ", "narrower lower ⱦ")
}
