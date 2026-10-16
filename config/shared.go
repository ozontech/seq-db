package config

import "github.com/alecthomas/units"

// MaxGrpcMessageSizeBytes is the max size of a single unary gRPC message
// (request or response) accepted by and sent from store and proxy servers.
const MaxGrpcMessageSizeBytes = 1024 * int(units.MiB)

var (
	IndexWorkers  int
	FetchWorkers  int
	ReaderWorkers int

	CaseSensitive = false
	SkipFsync     = false

	MaxFetchSizeBytes = 4 * units.MiB

	MaxRequestedDocuments = 100_000 // maximum number of documents that can be requested in one fetch request

	MaxRegexTokensCheck int

	FailPartialResponse = false
)
