package pattern

/*
 Finds substrings in a string in a fast (linear way)
 Uses prefix function (kinda KMP algorithm)
 For fast usage precalc prefFunc (via calcPrefFunc) for every substring
 Then, call 'findSequence' and it will try to found all substrings in O(len(string)) (linear time!)
*/

type substring struct {
	val      []byte
	prefFunc []int32
}

func newSubstringPattern(str []byte) *substring {
	s := substring{val: str, prefFunc: make([]int32, len(str))}
	s.calcPrefFunc()
	return &s
}

func (s *substring) calcPrefFunc() {
	curPrefFunc := int32(0)
	for i, b := range s.val[1:] {
		for curPrefFunc > 0 && b != s.val[curPrefFunc] {
			curPrefFunc = s.prefFunc[curPrefFunc-1]
		}
		if b == s.val[curPrefFunc] {
			curPrefFunc++
		}
		s.prefFunc[i+1] = curPrefFunc
	}
}

func findSubstring(s []byte, to *substring) int {
	curPrefFunc := int32(0)
	for i, b := range s {
		for curPrefFunc > 0 && b != to.val[curPrefFunc] {
			curPrefFunc = to.prefFunc[curPrefFunc-1]
		}
		if b == to.val[curPrefFunc] {
			curPrefFunc++
		}
		if curPrefFunc == int32(len(to.val)) {
			return i + 1
		}
	}
	return -1
}

func findSequence(s []byte, to []*substring) int {
	for cur := 0; cur < len(to); cur++ {
		end := findSubstring(s, to[cur])
		if end == -1 {
			return cur
		}
		s = s[end:]
	}
	return len(to)
}
