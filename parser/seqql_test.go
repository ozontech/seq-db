package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTrimOuterParens(t *testing.T) {
	test := func(input, expected string) {
		t.Helper()
		require.Equal(t, expected, trimOuterParens(input))
	}

	// Wraps the whole query: parentheses are stripped.
	test("(* | fields level)", "* | fields level")
	test("(* )", "*")
	test("( * | fields level )", "* | fields level")

	// Multiple outer layers are all stripped.
	test("((a:1))", "a:1")
	test("((* | fields level))", "* | fields level")
	test("(((* | fields level)))", "* | fields level")
	test("((( * )))", "*")
	test("(( (a:1) | fields b ))", "(a:1) | fields b")

	// No wrapping parentheses: nothing changes.
	test("* | fields level", "* | fields level")
	test("*", "*")
	test("(a:1) | fields b", "(a:1) | fields b")
	test("a:1 | (fields b)", "a:1 | (fields b)")

	// Parentheses that close before the end are not outer.
	test("((a:1) | fields b)", "(a:1) | fields b")

	// Empty/short input.
	test("", "")
	test("()", "")
	test("(", "(")
	test(")", ")")
}
