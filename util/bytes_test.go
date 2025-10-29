package util

import (
	"math"
	"testing"
)

// Добавлены проверки границ и больших значений.
func TestByteToHumanReadable(t *testing.T) {
	tests := []struct {
		name     string
		input    uint64
		expected string
	}{
		{
			name:     "ZeroBytes",
			input:    0,
			expected: "0 B",
		},
		{
			name:     "Bytes",
			input:    512,
			expected: "512 B",
		},
		{
			name:     "Exactly1KiB",
			input:    1024,
			expected: "1 KiB",
		},
		{
			name:     "JustBelow1MiB",
			input:    1024*1024 - 1,
			expected: "1023 KiB",
		},
		{
			name:     "Exactly1MiB",
			input:    1024 * 1024,
			expected: "1 MiB",
		},
		{
			name:     "Megabytes",
			input:    10 * 1024 * 1024,
			expected: "10 MiB",
		},
		{
			name:     "Gigabytes",
			input:    15 * 1024 * 1024 * 1024,
			expected: "15 GiB",
		},
		{
			name:     "Terabytes",
			input:    5 * 1024 * 1024 * 1024 * 1024,
			expected: "5 TiB",
		},
		{
			name:     "Petabytes",
			input:    2 * 1024 * 1024 * 1024 * 1024 * 1024,
			expected: "2 PiB",
		},
		{
			name:     "Exabytes",
			input:    1 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024,
			expected: "1 EiB",
		},
		{
			name:     "SmallFractionalKB",
			input:    1025,
			expected: "1 KiB",
		},
		{
			name:     "VeryLargeValue_NoOverflow",
			input:    math.MaxUint64,
			expected: "15 EiB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ByteToHumanReadable(tt.input)
			if result != tt.expected {
				t.Errorf("ByteToHumanReadable(%d): expected %s, got %s", tt.input, tt.expected, result)
			}
		})
	}
}
