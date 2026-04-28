package indexer

import (
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tokenizer"
)

const (
	metaDataVersion1 uint16 = 1 // milliseconds MID support (no longer supported)
	metaDataVersion2 uint16 = 2 // nanoseconds MID support

	metaDataVersion = metaDataVersion2
)

type MetaData struct {
	ID seq.ID
	// Size of an uncompressed document in bytes.
	Size   uint32
	Tokens []tokenizer.MetaToken
}

// String used in tests for human-readable output.
func (m MetaData) String() string {
	return fmt.Sprintf("ID: %s, Size: %d, Tokens: %s", m.ID, m.Size, m.Tokens)
}

const metadataMagic = uint16(0x3F7C) // 2 magic bytes of the binary encoded metadata.

func IsItBinaryEncodedMetaData(b []byte) bool {
	if len(b) < 2 {
		return false
	}
	return binary.LittleEndian.Uint16(b) == metadataMagic
}

func (m *MetaData) MarshalBinaryTo(b []byte) []byte {
	// Append "magic bytes" to determine that this is binary encoded metadata.
	b = binary.LittleEndian.AppendUint16(b, metadataMagic)

	// Append current binary version of the metadata.
	b = binary.LittleEndian.AppendUint16(b, metaDataVersion)

	// Encode seq.ID.
	b = binary.LittleEndian.AppendUint64(b, uint64(m.ID.MID))
	b = binary.LittleEndian.AppendUint64(b, uint64(m.ID.RID))

	// Encode BlockLength.
	b = binary.LittleEndian.AppendUint32(b, m.Size)

	// Encode tokens.
	toksLen := len(m.Tokens)
	b = binary.LittleEndian.AppendUint32(b, uint32(toksLen))
	for i := 0; i < toksLen; i++ {
		b = m.Tokens[i].MarshalBinaryTo(b)
	}

	return b
}

func (m *MetaData) UnmarshalBinary(b []byte) error {
	if !IsItBinaryEncodedMetaData(b) {
		return fmt.Errorf("invalid metadata magic bytes")
	}
	b = b[2:]

	version := binary.LittleEndian.Uint16(b)
	b = b[2:]

	switch version {
	case metaDataVersion1:
		return fmt.Errorf("version: %d no longer supported", version)
	case metaDataVersion2:
		return m.unmarshal(b)
	default:
		return fmt.Errorf("unimplemented metadata version: %d", version)
	}
}

func (m *MetaData) unmarshal(b []byte) error {
	// Decode seq.ID.
	m.ID.MID = seq.MID(binary.LittleEndian.Uint64(b))
	b = b[8:]
	m.ID.RID = seq.RID(binary.LittleEndian.Uint64(b))
	b = b[8:]

	// Decode uncompressed document size.
	m.Size = binary.LittleEndian.Uint32(b)
	b = b[4:]

	// Decode tokens length.
	toksLen := binary.LittleEndian.Uint32(b)
	b = b[4:]

	// Decode tokens.
	m.Tokens = m.Tokens[:0]
	m.Tokens = slices.Grow(m.Tokens, int(toksLen))
	var err error
	for i := uint32(0); i < toksLen; i++ {
		var token tokenizer.MetaToken
		b, err = token.UnmarshalBinary(b)
		if err != nil {
			return err
		}
		m.Tokens = append(m.Tokens, token)
	}
	return nil
}
