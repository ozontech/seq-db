package main

import (
	"github.com/ozontech/seq-db/frac"
)

func unpackInfo(result []byte) *frac.Info {
	result = result[4:]
	info := &frac.Info{}
	info.Load(result)
	return info
}
