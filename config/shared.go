package config

import "github.com/alecthomas/units"

var (
	IndexWorkers  int
	FetchWorkers  int
	ReaderWorkers int

	CaseSensitive = false
	SkipFsync     = false

	MaxFetchSizeBytes = 4 * units.MB

	MaxRequestedDocuments = 100_000 // maximum number of documents that can be requested in one fetch request

	UseSeqQLByDefault = false
)
