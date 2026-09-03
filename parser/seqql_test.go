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

	// Double-quoted strings.
	test(`(((body: ":)" | fields level)))`, `body: ":)" | fields level`)
	test(`(msg: "a ( b ) c")`, `msg: "a ( b ) c"`)
	test(`((msg: "(test)"))`, `msg: "(test)"`)
	test(`((msg: "open ( no close"))`, `msg: "open ( no close"`)
	test(`(msg: ")")`, `msg: ")"`)
	test(`(msg: "(" | fields b)`, `msg: "(" | fields b`)

	// Single-quoted strings.
	test(`(msg: '(' | fields b)`, `msg: '(' | fields b`)
	test(`(msg: 'foo ) bar')`, `msg: 'foo ) bar'`)
	test(`((msg: '))'))`, `msg: '))'`)

	// Escaped quotes inside strings.
	test(`(msg: "a\"b)" | fields b)`, `msg: "a\"b)" | fields b`)
	test(`((msg: "\""))`, `msg: "\""`)
	test(`(msg: 'it\'s ) ok' | fields b)`, `msg: 'it\'s ) ok' | fields b`)

	// Single-quote inside double-quotes.
	test(`((msg: "\"'"))`, `msg: "\"'"`)

	// Raw strings (backtick).
	test("(msg: `a ( b ) c`)", "msg: `a ( b ) c`")
	test("((msg: `)` | fields b))", "msg: `)` | fields b")
}
