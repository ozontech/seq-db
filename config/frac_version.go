package config

type BinaryDataVersion uint16

const (
	// BinaryDataV0 - initial version
	BinaryDataV0 BinaryDataVersion = iota

	// BinaryDataV1 - support RIDs encoded without varint
	BinaryDataV1

	// BinaryDataV2 - MIDs stored in nanoseconds
	BinaryDataV2

	// BinaryDataV3 - `.index` file is split across several files
	// storing specific sections: `.info`, `.offsets`, `.tokens`, `.ids`, `.lids`.
	//
	// Also in this version we've changed the binary layout of section storing
	// info block. As a result we store info as a plain JSON without additional registry.
	BinaryDataV3

	// BinaryDataV4 - delta bitpack encoded MIDs and LIDs
	BinaryDataV4
)

const CurrentFracVersion = BinaryDataV4
