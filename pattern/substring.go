package pattern

import (
	"bytes"
)

func findSequence(haystack []byte, needles [][]byte) int {
	for cur := range needles {
		val := needles[cur]
		start := bytes.Index(haystack, needles[cur])
		if start == -1 {
			return cur
		}
		haystack = haystack[start+len(val):]
	}
	return len(needles)
}
