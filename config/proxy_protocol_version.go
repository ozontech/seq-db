package config

import "strconv"

type ProxyProtocolVersion int

const (
	// ProxyProtocolVersion1 default version (same as proxy reports no version)
	ProxyProtocolVersion1 ProxyProtocolVersion = 1
	// ProxyProtocolVersion2 proxy now supports new QPR format (compressed sorted columnar)
	ProxyProtocolVersion2 ProxyProtocolVersion = 2
)

var CurrentProxyProtocolVersion = ProxyProtocolVersion2

func (p ProxyProtocolVersion) String() string {
	return strconv.Itoa(int(p))
}

func ParseProxyProtocolVersion(s string) ProxyProtocolVersion {
	if s == ProxyProtocolVersion1.String() {
		return ProxyProtocolVersion1
	}
	return ProxyProtocolVersion2
}
