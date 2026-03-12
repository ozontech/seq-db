package util

import "unsafe"

const (
	SizeOfString  = int(unsafe.Sizeof(*new(string)))
	SizeOfUint32  = int(unsafe.Sizeof(*new(uint32)))
	SizeOfUint64  = int(unsafe.Sizeof(*new(uint64)))
	SizeOfPointer = int(unsafe.Sizeof(new(int)))
	SizeOfFloat64 = int(unsafe.Sizeof(*new(float64)))
)
