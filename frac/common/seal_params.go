package common

type SealParams struct {
	IDsZstdLevel           int
	LIDsZstdLevel          int
	TokenListZstdLevel     int
	DocsPositionsZstdLevel int
	TokenTableZstdLevel    int

	DocBlocksZstdLevel  int // DocBlocksZstdLevel is the zstd compress level of each document block.
	LIDBlockSize        int
	LidsBitmapThreshold int // LidsBitmapThreshold is the minimum number of LIDs in the lid list to serialize as bitmap.
	DocBlockSize        int // DocBlockSize is decompressed payload size of document block.
}
