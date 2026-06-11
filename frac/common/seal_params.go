package common

type SealParams struct {
	// DisableIndexCompression disables compression
	// of index components (lids, ids, etc.)
	DisableIndexCompression bool

	IDsZstdLevel           int
	LIDsZstdLevel          int
	TokenListZstdLevel     int
	DocsPositionsZstdLevel int
	TokenTableZstdLevel    int
	DocBlocksZstdLevel     int // DocBlocksZstdLevel is the zstd compress level of each document block.

	LIDBlockSize   int
	TokenBlockSize int
	TokenFreqThreshold int // TokenFreqThreshold Min lids count to store frequency for a token.
	DocBlockSize   int // DocBlockSize is decompressed payload size of document block.
}
