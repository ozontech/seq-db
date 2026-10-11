package processor

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/parser"
)

func testLiteral(s string) *parser.Literal {
	return &parser.Literal{Field: "field", Terms: []parser.Term{{Kind: parser.TermText, Data: s}}}
}

func TestTokenExprCacheDisabled(t *testing.T) {
	c := NewTokenExprCache(0)
	assert.Nil(t, c)

	// nil cache methods must be no-ops
	_, ok := c.Get("frac", testLiteral("val"))
	assert.False(t, ok)
	c.Put("frac", testLiteral("val"), []uint32{1, 2, 3})
}

func TestTokenExprCacheHit(t *testing.T) {
	c := NewTokenExprCache(1024)
	token := testLiteral("*abc")

	c.Put("frac1", token, []uint32{7})
	tids, ok := c.Get("frac1", token)
	assert.True(t, ok)
	assert.Equal(t, []uint32{7}, tids)

	// the same token in another fraction must be a miss
	_, ok = c.Get("frac2", token)
	assert.False(t, ok)
}

func TestTokenExprCacheEmptyResult(t *testing.T) {
	c := NewTokenExprCache(1024)
	token := testLiteral("*abc")

	c.Put("frac", token, nil)
	tids, ok := c.Get("frac", token)
	assert.True(t, ok)
	assert.Empty(t, tids)
}

func TestTokenExprCacheFull(t *testing.T) {
	c := NewTokenExprCache(8) // holds at most 2 tids
	a, b := testLiteral("*a"), testLiteral("*b")

	c.Put("frac", a, []uint32{1})
	c.Put("frac", b, []uint32{1, 2})
	_, ok := c.Get("frac", b)
	assert.False(t, ok) // dropped: cache is full

	tids, ok := c.Get("frac", a)
	assert.True(t, ok)
	assert.Equal(t, []uint32{1}, tids)
}
