package config

import (
	"fmt"
	"strconv"
)

type ProxyProtocolVersion int

const (
	// ProxyProtocolVersion1 default version (same as proxy reports no version)
	ProxyProtocolVersion1 ProxyProtocolVersion = 1
	// ProxyProtocolVersion2 proxy now supports compressed sorted columnar QPR
	ProxyProtocolVersion2 ProxyProtocolVersion = 2
)

var CurrentProxyProtocolVersion = ProxyProtocolVersion2

func (p ProxyProtocolVersion) String() string {
	return strconv.Itoa(int(p))
}

func ParseProxyProtocolVersion(s string) (ProxyProtocolVersion, error) {
	version, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("parse proxy protocol version: %w", err)
	}

	p := ProxyProtocolVersion(version)
	switch p {
	case ProxyProtocolVersion1, ProxyProtocolVersion2:
		return p, nil
	default:
		return 0, fmt.Errorf("unsupported proxy protocol version %d", p)
	}
}
