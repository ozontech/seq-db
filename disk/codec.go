package disk

import (
	"fmt"

	"github.com/pierrec/lz4/v4"

	"github.com/ozontech/seq-db/util"
	"github.com/ozontech/seq-db/zstd"
)

const (
	CodecNo Codec = iota
	CodecLZ4
	CodecZSTD
)

type Codec byte

func (codec Codec) decompressBlock(rawLen int, src, dst []byte) ([]byte, error) {
	var err error
	dst = util.EnsureSliceSize(dst, rawLen)
	switch codec {
	case CodecLZ4:
		_, err = lz4.UncompressBlock(src, dst)
	case CodecZSTD:
		dst = dst[:0]
		dst, err = zstd.Decompress(src, dst)
	default:
		return nil, fmt.Errorf("unimplemented codec %d", codec)
	}

	return dst, err
}
