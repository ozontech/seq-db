package config

type StoreProtocolVersion uint8

const (
	// StoreProtocolVersion1 MID is in milliseconds
	StoreProtocolVersion1 StoreProtocolVersion = 1
	// StoreProtocolVersion2 MID is in nanoseconds
	StoreProtocolVersion2 StoreProtocolVersion = 2
)

func (p StoreProtocolVersion) String() string {
	switch p {
	case StoreProtocolVersion1:
		return "1"
	case StoreProtocolVersion2:
		return "2"
	default:
		return "1" // Default to protocol version 1 (milliseconds)
	}
}

// ParseStoreProtocolVersion parses a protocol version string and returns the corresponding StoreProtocolVersion.
func ParseStoreProtocolVersion(s string) StoreProtocolVersion {
	switch s {
	case "1":
		return StoreProtocolVersion1
	case "2":
		return StoreProtocolVersion2
	default:
		return StoreProtocolVersion1 // Default to protocol version 1 (milliseconds)
	}
}
