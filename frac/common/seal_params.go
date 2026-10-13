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

	LIDBlockSize                 int
	LIDsBitmapThreshold          int // LIDsBitmapThreshold is the minimum number of LIDs in the lid list to serialize as bitmap.
	TokenBlockSize               int
	TokenFreqThresholdPercentage float64
	DocBlockSize                 int // DocBlockSize is decompressed payload size of document block.
}
