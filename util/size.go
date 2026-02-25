package util

import "unsafe"

const (
	SizeOfString  = uint64(unsafe.Sizeof(*new(string)))
	SizeOfUint32  = uint64(unsafe.Sizeof(*new(uint32)))
	SizeOfUint64  = uint64(unsafe.Sizeof(*new(uint64)))
	SizeOfPointer = uint64(unsafe.Sizeof(new(int)))
	SizeOfFloat64 = uint64(unsafe.Sizeof(*new(float64)))
)
