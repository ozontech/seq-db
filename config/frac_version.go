package config

type BinaryDataVersion uint16

const (
	// BinaryDataV0 - initial version
	BinaryDataV0 BinaryDataVersion = iota
	// BinaryDataV1 - support RIDs encoded without varint
	BinaryDataV1
	// BinaryDataV2 - MIDs stored in nanoseconds
	BinaryDataV2
	// BinaryDataV3 - MIDs and LIDs encoded in bitpack, variable LID block size
	BinaryDataV3
)

const CurrentFracVersion = BinaryDataV3
