package config

type BinaryDataVersion uint16

const (
	// BinaryDataV0 - initial version
	BinaryDataV0 BinaryDataVersion = iota

	// BinaryDataV1 - support RIDs encoded without varint
	BinaryDataV1

	// BinaryDataV2 - MIDs stored in nanoseconds
	BinaryDataV2

	// BinariDataV3 - `.index` file is split across several files
	// storing specific sections: `.info`, `.offsets`, `.tokens`, `.ids`, `.lids`
	BinaryDataV3
)

const CurrentFracVersion = BinaryDataV3
