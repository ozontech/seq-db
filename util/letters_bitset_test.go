package util

import (
	"testing"
)

func TestLettersBitset_ContainsAll(t *testing.T) {
	tests := []struct {
		name     string
		set      string
		required string
		expected bool
	}{
		{
			name:     "empty set contains empty",
			set:      "",
			required: "",
			expected: true,
		},
		{
			name:     "set contains all its characters",
			set:      "abcd",
			required: "ab",
			expected: true,
		},
		{
			name:     "set does not contain missing character",
			set:      "abcd",
			required: "xyz",
			expected: false,
		},
		{
			name:     "set contains subset",
			set:      "abcdef",
			required: "bce",
			expected: true,
		},
		{
			name:     "set does not contain partial subset",
			set:      "abc",
			required: "abcd",
			expected: false,
		},
		{
			name:     "case insensitive",
			set:      "ABC",
			required: "abc",
			expected: true,
		},
		{
			name:     "case insensitive reverse",
			set:      "abc",
			required: "ABC",
			expected: true,
		},
		{
			name:     "mixed content - contains digits",
			set:      "abc123",
			required: "1",
			expected: true,
		},
		{
			name:     "mixed content - does not contain digit",
			set:      "abc",
			required: "1",
			expected: false,
		},
		{
			name:     "mixed content - contains special",
			set:      "abc!@",
			required: "!",
			expected: true,
		},
		{
			name:     "mixed content - does not contain special",
			set:      "abc",
			required: "!",
			expected: false,
		},
		{
			name:     "russian characters",
			set:      "а",
			required: string([]byte{208}),
			expected: true,
		},
		{
			name:     "russian not present",
			set:      "abc",
			required: string([]byte{208}),
			expected: false,
		},
		{
			name:     "exact match",
			set:      "abc",
			required: "abc",
			expected: true,
		},
		{
			name:     "single character",
			set:      "abc",
			required: "b",
			expected: true,
		},
		{
			name:     "single character missing",
			set:      "abc",
			required: "x",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			set := NewLettersBitset([]byte(tt.set))
			required := NewLettersBitset([]byte(tt.required))
			result := set.ContainsAll(required)

			if result != tt.expected {
				t.Errorf("set.ContainsAll(required) = %v, want %v (set=%q, required=%q)",
					result, tt.expected, tt.set, tt.required)
			}
		})
	}
}
