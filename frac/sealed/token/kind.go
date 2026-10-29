package token

import (
	"bytes"
	"cmp"
	"slices"

	"github.com/ozontech/seq-db/util"
)

type Kind uint8

const (
	KindString Kind = iota
	KindInt64
)

func Classify(tok []byte) (Kind, int64) {
	if v, ok := util.ParseCanonicalInt64(tok); ok {
		return KindInt64, v
	}
	return KindString, 0
}

func Compare(a, b []byte) int {
	ka, va := Classify(a)
	kb, vb := Classify(b)
	return CompareClassified(a, ka, va, b, kb, vb)
}

func CompareClassified(a []byte, ka Kind, va int64, b []byte, kb Kind, vb int64) int {
	if ka != kb {
		return cmp.Compare(ka, kb)
	}
	if ka == KindInt64 {
		return cmp.Compare(va, vb)
	}
	return bytes.Compare(a, b)
}

func SortTIDs(tids []uint32, val func(uint32) []byte) {
	type intTID struct {
		v   int64
		tid uint32
	}

	var ints []intTID
	strs := tids[:0]
	for _, tid := range tids {
		if kind, v := Classify(val(tid)); kind == KindInt64 {
			ints = append(ints, intTID{v: v, tid: tid})
		} else {
			strs = append(strs, tid)
		}
	}

	slices.SortFunc(strs, func(a, b uint32) int { return bytes.Compare(val(a), val(b)) })
	slices.SortFunc(ints, func(a, b intTID) int { return cmp.Compare(a.v, b.v) })

	for i, it := range ints {
		tids[len(strs)+i] = it.tid
	}
}
