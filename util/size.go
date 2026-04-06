package util

import "unsafe"

const (
	SizeOfString  = int(unsafe.Sizeof(""))
	SizeOfUint32  = int(unsafe.Sizeof(uint32(0)))
	SizeOfUint64  = int(unsafe.Sizeof(uint64(0)))
	SizeOfPointer = int(unsafe.Sizeof(int(0)))
	SizeOfFloat64 = int(unsafe.Sizeof(float64(0.0)))
)
