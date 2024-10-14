package node

type LessFn func(uint32, uint32) bool

var lessAsc LessFn = func(u1, u2 uint32) bool { return u1 < u2 }
var lessDesc LessFn = func(u1, u2 uint32) bool { return u2 < u1 }

func GetLessFn(reverse bool) LessFn {
	if reverse {
		return lessDesc
	}
	return lessAsc
}
