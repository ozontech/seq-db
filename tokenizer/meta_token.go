package tokenizer

import (
	"encoding/binary"
	"fmt"
)

type MetaToken struct {
	Key   []byte
	Value []byte
}

func (m *MetaToken) MarshalBinaryTo(b []byte) []byte {
	b = binary.LittleEndian.AppendUint32(b, uint32(len(m.Key)))
	b = append(b, m.Key...)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(m.Value)))
	b = append(b, m.Value...)
	return b
}

func (m *MetaToken) UnmarshalBinary(b []byte) ([]byte, error) {
	keyLen := binary.LittleEndian.Uint32(b)
	b = b[4:]
	if int(keyLen) > len(b) {
		return nil, fmt.Errorf("malformed key")
	}
	m.Key = b[:keyLen]
	b = b[keyLen:]

	valueLen := binary.LittleEndian.Uint32(b)
	b = b[4:]
	if int(valueLen) > len(b) {
		return nil, fmt.Errorf("malformed value")
	}
	m.Value = b[:valueLen]
	b = b[valueLen:]
	return b, nil
}

// String used in tests for human-readable output.
func (m MetaToken) String() string {
	return fmt.Sprintf("(%s: %s)", m.Key, m.Value)
}
