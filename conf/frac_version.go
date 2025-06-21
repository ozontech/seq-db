package conf

type BinaryDataVersion uint16

const (
	// BinaryDataV0 - initial version
	BinaryDataV0 BinaryDataVersion = iota
	// BinaryDataV1 - support RIDs encoded without varint
	BinaryDataV1
)
