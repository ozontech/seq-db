package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"unsafe"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

type WalRecord struct {
	ContainsData bool
	Data         WalBlock
	Offset       int64
	Size         int64
	Corruptions  uint64
	Err          error
}

type WalReader struct {
	limiter      *ReadLimiter
	reader       io.ReaderAt
	headerOffset int64 // offset where actual data starts (WalHeaderSize for new format)
	baseFileName string
}

func NewWalReader(limiter *ReadLimiter, reader io.ReaderAt, baseFileName string) (*WalReader, error) {
	header := make([]byte, WalHeaderSize)
	n, err := limiter.ReadAt(reader, header, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("failed to read WAL header: %w", err)
	}
	if n < WalHeaderSize {
		return nil, fmt.Errorf("WAL file too short: expected at least %d bytes, got %d", WalHeaderSize, n)
	}
	magic := binary.LittleEndian.Uint32(header[0:unsafe.Sizeof(WalMagic)])
	if magic != WalMagic {
		return nil, fmt.Errorf("invalid WAL magic: expected 0x%X, got 0x%X", WalMagic, magic)
	}
	version := header[4]
	if version != WalCurrentVersion {
		return nil, fmt.Errorf("unknown WAL version: %d (supported: %d)", version, WalCurrentVersion)
	}

	return &WalReader{
		limiter:      limiter,
		reader:       reader,
		headerOffset: WalHeaderSize,
		baseFileName: baseFileName,
	}, nil
}

// Entries iterates through WAL file. Corrupted entries are skipped and never propagated to a client.
// Corruption ranges are logged with "from" and "to" offsets.
func (r *WalReader) Entries() iter.Seq[WalRecord] {
	return func(yield func(WalRecord) bool) {
		offset := align(r.headerOffset)

		var corruptionStart int64 = -1
		var corruptions uint64
		logCorruptionEnd := func(offset int64) {
			if corruptionStart >= 0 {
				logger.Error("WAL file corrupted",
					zap.String("fraction", r.baseFileName),
					zap.Int64("from", corruptionStart),
					zap.Int64("to", offset))
				corruptions++
				corruptionStart = -1
			}
		}
		startCorruptionTracking := func(offset int64) {
			if corruptionStart < 0 {
				corruptionStart = offset
			}
		}

		headerBuf := make([]byte, WalBlockHeaderLen)
		for {
			n, err := r.limiter.ReadAt(r.reader, headerBuf, offset)

			if err != nil && !errors.Is(err, io.EOF) {
				yield(WalRecord{Offset: offset, Corruptions: corruptions, Err: err})
				return
			}

			if errors.Is(err, io.EOF) || n < WalBlockHeaderLen {
				if n > 0 {
					// we read at least one byte - start corruption if not already started
					startCorruptionTracking(offset)
				}
				logCorruptionEnd(offset + int64(n))
				yield(WalRecord{Offset: offset, Corruptions: corruptions})
				return
			}

			if !IsWalBlock(headerBuf) {
				startCorruptionTracking(offset)
				offset += WalBlockAlignment
				continue
			}

			mb := WalBlock(headerBuf)

			if !mb.IsHeaderCorrect() {
				startCorruptionTracking(offset)
				offset += WalBlockAlignment
				continue
			}

			// header is correct, try to read the payload
			blockLen := int64(mb.FullLen())
			blockBuf := make([]byte, blockLen)
			n, err = r.limiter.ReadAt(r.reader, blockBuf, offset)

			if err != nil && !errors.Is(err, io.EOF) {
				yield(WalRecord{Offset: offset, Corruptions: corruptions, Err: err})
				return
			}

			if errors.Is(err, io.EOF) || int64(n) < blockLen {
				startCorruptionTracking(offset)
				logCorruptionEnd(offset + int64(n))
				yield(WalRecord{Offset: offset, Corruptions: corruptions})
				return
			}

			mb = blockBuf

			if !mb.IsPayloadCorrect() {
				startCorruptionTracking(offset)
				offset = align(offset + blockLen)
				continue
			}

			logCorruptionEnd(offset)

			entry := WalRecord{
				ContainsData: true,
				Data:         mb,
				Offset:       offset,
				Size:         blockLen,
				Corruptions:  corruptions,
			}

			if !yield(entry) {
				return
			}

			offset = align(offset + blockLen)
		}
	}
}

// align aligns provided offset to WalBlockAlignment
func align(offset int64) int64 {
	return (offset + WalBlockAlignment - 1) &^ (WalBlockAlignment - 1)
}
