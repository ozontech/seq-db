package token

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelectEntries(t *testing.T) {
	single := &FieldData{
		MinVal: "abc",
		Entries: []*TableEntry{
			{MaxVal: "def"},
		},
	}
	triple := &FieldData{
		MinVal: "ab",
		Entries: []*TableEntry{
			{MaxVal: "bb"},
			{MaxVal: "bd"},
			{MaxVal: "cd"},
		},
	}
	table := Table{
		"single": single,
		"triple": triple,
	}
	emptyEntries := []*TableEntry{}

	assert.Equal(t, single.Entries, table.SelectEntries("single", "a"))
	assert.Equal(t, single.Entries, table.SelectEntries("single", "d"))
	assert.Equal(t, single.Entries, table.SelectEntries("single", "abc"))
	assert.Equal(t, single.Entries, table.SelectEntries("single", "def"))
	assert.Equal(t, emptyEntries, table.SelectEntries("single", "aa"))
	assert.Equal(t, emptyEntries, table.SelectEntries("single", "df"))

	assert.Equal(t, triple.Entries[:1], table.SelectEntries("triple", "a"))
	assert.Equal(t, triple.Entries, table.SelectEntries("triple", "b"))
	assert.Equal(t, triple.Entries[:1], table.SelectEntries("triple", "ba"))
	assert.Equal(t, triple.Entries[:2], table.SelectEntries("triple", "bb"))
	assert.Equal(t, triple.Entries[1:2], table.SelectEntries("triple", "bc"))
	assert.Equal(t, triple.Entries[1:3], table.SelectEntries("triple", "bd"))
	assert.Equal(t, triple.Entries[2:3], table.SelectEntries("triple", "be"))
	assert.Equal(t, triple.Entries[2:3], table.SelectEntries("triple", "ca"))

	assert.Equal(t, triple.Entries[1:2], table.SelectEntries("triple", "bbb"))
	assert.Equal(t, triple.Entries[2:3], table.SelectEntries("triple", "bee"))
	assert.Equal(t, emptyEntries, table.SelectEntries("triple", "aaa"))
	assert.Equal(t, emptyEntries, table.SelectEntries("triple", "xyz"))
}
