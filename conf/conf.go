package conf

import "github.com/ozontech/seq-db/consts"

var (
	BulkWorkers  = 32
	IndexWorkers = 1
	FetchWorkers = 32
	ExecWorkers  = 128

	CaseSensitive = false
	SkipFsync     = false

	ReaderWorkers = consts.ReaderWorkers

	MaxFetchSizeBytes = 4 * consts.MB

	MaxRequestedDocuments = consts.DefaultMaxRequestedDocuments
)
