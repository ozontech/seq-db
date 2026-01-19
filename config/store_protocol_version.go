package config

import (
	"strconv"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

type StoreProtocolVersion int

const (
	// StoreProtocolVersion1 MID is in milliseconds
	StoreProtocolVersion1 StoreProtocolVersion = 1
	// StoreProtocolVersion2 MID is in nanoseconds
	StoreProtocolVersion2 StoreProtocolVersion = 2
)

func (p StoreProtocolVersion) String() string {
	return strconv.Itoa(int(p))
}

// ParseStoreProtocolVersion parses a protocol version string and returns the corresponding StoreProtocolVersion.
func ParseStoreProtocolVersion(s string) StoreProtocolVersion {
	version, err := strconv.Atoi(s)
	if err != nil {
		logger.Error("failed to parse protocol", zap.Error(err), zap.String("value", s))
		return StoreProtocolVersion1
	}
	return StoreProtocolVersion(version)
}
