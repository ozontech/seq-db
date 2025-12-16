package docsfilter

import (
	"encoding/binary"
	"fmt"

	"github.com/ozontech/seq-db/seq"
)

type docsFilterBinVersion uint8

const (
	docsFilterBinVersion1 docsFilterBinVersion = iota + 1
)

var availableVersions = map[docsFilterBinVersion]struct{}{
	docsFilterBinVersion1: {},
}

func marshalDocsFilter(dst []byte, in *DocsFilterBin) []byte {
	dst = append(dst, uint8(docsFilterBinVersion1))
	dst = marshalLIDsBlock(dst, in.LIDs)

	return dst
}

func marshalLIDsBlock(dst []byte, in []seq.LID) []byte {
	dst = binary.BigEndian.AppendUint64(dst, uint64(len(in)))

	prev := seq.LID(0)
	for i := range len(in) {
		lid := in[i]
		deltaLID := lid - prev
		prev = lid
		dst = binary.BigEndian.AppendUint32(dst, uint32(deltaLID))
	}

	return dst
}

const minLIDsFIlterBytesLen = 13 // 1 byte lidsBinVersion + 8 byte number of LIDs + 4 * N bytes LIDs

func unmarshalDocsFilter(dst *DocsFilterBin, src []byte) (_ []byte, err error) {
	if len(src) < minLIDsFIlterBytesLen {
		return nil, fmt.Errorf("invalid LIDs filter format; want %d bytes, got %d", minLIDsFIlterBytesLen, len(src))
	}

	version := docsFilterBinVersion(src[0])
	src = src[1:]
	if _, ok := availableVersions[version]; !ok {
		return nil, fmt.Errorf("invalid LIDs binary version: %d", version)
	}

	dst.LIDs, src, err = unmarshalLIDsBlock(dst.LIDs, src)
	if err != nil {
		return src, err
	}

	return src, nil
}

func unmarshalLIDsBlock(dst []seq.LID, src []byte) ([]seq.LID, []byte, error) {
	numberOfLIDs := int(binary.BigEndian.Uint64(src))
	src = src[8:]
	if numberOfLIDs > len(src)/4 {
		return nil, src, fmt.Errorf("invalid LIDs block length %d; want %d", len(src)/4, numberOfLIDs)
	}

	prevLID := uint32(0)
	for range numberOfLIDs {
		v := binary.BigEndian.Uint32(src)
		lid := prevLID + v
		prevLID = lid
		src = src[4:]
		dst = append(dst, seq.LID(lid))
	}

	if len(src) > 0 {
		return dst, src, fmt.Errorf("unexpected tail when unmarshaling LIDs delta")
	}

	return dst, src, nil
}
