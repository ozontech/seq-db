package frac

import (
	"sync"
)

type Base struct {
	Info         Info
	BaseFileName string
	DocsReader   DocsReader

	UseMu    sync.RWMutex
	Suicided bool
}
