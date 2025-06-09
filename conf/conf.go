package conf

import (
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/limits"
)

func init() {
	IndexWorkers = limits.NumCPU
	FetchWorkers = limits.NumCPU
	ReaderWorkers = limits.NumCPU
}

var (
	IndexWorkers  int
	FetchWorkers  int
	ReaderWorkers int

	CaseSensitive = false
	SkipFsync     = false

	MaxFetchSizeBytes = 4 * consts.MB

	MaxRequestedDocuments = 100_000 // maximum number of documents that can be requested in one fetch request

	UseSeqQLByDefault = false
)

type BinaryDataVersion uint16

const (
	// BinaryDataV0 - initial version
	BinaryDataV0 BinaryDataVersion = iota
	// BinaryDataV1 - support RIDs encoded without varint
	BinaryDataV1
)
