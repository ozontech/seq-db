package tokenizer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPathTokenizer(t *testing.T) {
	const field = "path"

	tests := []struct {
		title, value string
		maxTokenSize int
		tokenizer    *PathTokenizer
		expected     []MetaToken
	}{
		{
			title:        "empty value",
			value:        "",
			maxTokenSize: 100,
			tokenizer:    NewPathTokenizer(100, true, true),
			expected:     []MetaToken{newMetaToken(field, "")},
		},
		{
			title:        "slashes only",
			value:        "///",
			maxTokenSize: 100,
			tokenizer:    NewPathTokenizer(100, true, true),
			expected: []MetaToken{
				newMetaToken(field, "/"),
				newMetaToken(field, "//"),
				newMetaToken(field, "///"),
			},
		},
		{
			title:        "simple",
			value:        "/One/Two/Three",
			maxTokenSize: 100,
			tokenizer:    NewPathTokenizer(100, true, true),
			expected: []MetaToken{
				newMetaToken(field, "/One"),
				newMetaToken(field, "/One/Two"),
				newMetaToken(field, "/One/Two/Three"),
			},
		},
		{
			title:        "last slash",
			value:        "/One/Two/Three/",
			maxTokenSize: 100,
			tokenizer:    NewPathTokenizer(100, true, true),
			expected: []MetaToken{
				newMetaToken(field, "/One"),
				newMetaToken(field, "/One/Two"),
				newMetaToken(field, "/One/Two/Three"),
				newMetaToken(field, "/One/Two/Three/"),
			},
		},
		{
			title:        "max length",
			value:        "/one/two/three/",
			maxTokenSize: 10,
			tokenizer:    NewPathTokenizer(100, true, false),
			expected:     []MetaToken{},
		},
		{
			title:        "max length default",
			value:        "/one/two/three/",
			maxTokenSize: 0,
			tokenizer:    NewPathTokenizer(10, true, false),
			expected:     []MetaToken{},
		},
		{
			title:        "partial indexing",
			value:        "/one/two/three/",
			maxTokenSize: 10,
			tokenizer:    NewPathTokenizer(100, true, true),
			expected: []MetaToken{
				newMetaToken(field, "/one"),
				newMetaToken(field, "/one/two"),
				newMetaToken(field, "/one/two/t"),
			},
		},
		{
			title:        "partial indexing default",
			value:        "/one/two/three/",
			maxTokenSize: 0,
			tokenizer:    NewPathTokenizer(10, true, true),
			expected: []MetaToken{
				newMetaToken(field, "/one"),
				newMetaToken(field, "/one/two"),
				newMetaToken(field, "/one/two/t"),
			},
		},
		{
			title:        "case sensitive",
			value:        "/OnE/tWo",
			maxTokenSize: 10,
			tokenizer:    NewPathTokenizer(10, false, true),
			expected: []MetaToken{
				newMetaToken(field, "/one"),
				newMetaToken(field, "/one/two"),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.title, func(t *testing.T) {
			tokens := tc.tokenizer.Tokenize([]MetaToken{}, []byte(field), []byte(tc.value), tc.maxTokenSize)
			assert.Equal(t, tc.expected, tokens)
		})
	}
}
