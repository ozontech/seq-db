// This code has been copied and modified from:
// https://github.com/VictoriaMetrics/VictoriaMetrics/blob/7551d799f740b2b255bd0b470ffc42dd6978a6b7/lib/encoding/zstd/zstd_pure.go

package zstd

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
)

var (
	decoder *zstd.Decoder

	mu sync.Mutex

	// av stores map[int]*zstd.Encoder for separating encoders with different compression level.
	av atomic.Value
)

func init() {
	r := make(map[int]*zstd.Encoder)
	av.Store(r)

	var err error
	decoder, err = zstd.NewReader(nil)
	if err != nil {
		panic(fmt.Errorf("BUG: failed to create ZSTD reader: %s", err))
	}
}

// Decompress appends decompressed src to dst and returns the result.
func Decompress(src, dst []byte) ([]byte, error) {
	return decoder.DecodeAll(src, dst)
}

// CompressLevel appends compressed src to dst and returns the result.
//
// The given compressionLevel is used for the compression.
func CompressLevel(src, dst []byte, compressionLevel int) []byte {
	e := getEncoder(compressionLevel)
	return e.EncodeAll(src, dst)
}

func getEncoder(compressionLevel int) *zstd.Encoder {
	r := av.Load().(map[int]*zstd.Encoder)
	e := r[compressionLevel]
	if e != nil {
		return e
	}

	mu.Lock()
	// Create the encoder under lock in order to prevent from wasted work
	// when concurrent goroutines create encoder for the same compressionLevel.
	r1 := av.Load().(map[int]*zstd.Encoder)
	if e = r1[compressionLevel]; e == nil {
		e = newEncoder(compressionLevel)
		r2 := make(map[int]*zstd.Encoder)
		for k, v := range r1 {
			r2[k] = v
		}
		r2[compressionLevel] = e
		av.Store(r2)
	}
	mu.Unlock()

	return e
}

func newEncoder(compressionLevel int) *zstd.Encoder {
	level := zstd.EncoderLevelFromZstd(compressionLevel)
	e, err := zstd.NewWriter(nil,
		zstd.WithEncoderCRC(false), // Disable CRC for performance reasons.
		zstd.WithEncoderLevel(level))
	if err != nil {
		panic(fmt.Errorf("BUG: failed to create ZSTD writer: %s", err))
	}
	return e
}
