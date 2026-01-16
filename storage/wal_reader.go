package storage

import (
	"errors"
	"io"
	"iter"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

type WalRecord struct {
	Data   MetaBlock
	Offset int64
	Size   int64
	Err    error
}

type WalReader struct {
	limiter      *ReadLimiter
	reader       io.ReaderAt
	headerOffset int64 // offset where actual data starts (WALHeaderSize for new format)
	baseFileName string
}

func NewWalReader(limiter *ReadLimiter, reader io.ReaderAt, baseFileName string) *WalReader {
	return &WalReader{
		limiter:      limiter,
		reader:       reader,
		headerOffset: WALHeaderSize,
		baseFileName: baseFileName,
	}
}

// Iter iterates through WAL file. Corrupted entries are skipped and never propagated to a client.
// Corruption ranges are logged with "from" and "to" offsets.
func (r *WalReader) Iter() iter.Seq[WalRecord] {
	return func(yield func(WalRecord) bool) {
		offset := nextBlockOffset(r.headerOffset)

		var corruptionStart int64 = -1
		logCorruptionEnd := func(offset int64) {
			if corruptionStart >= 0 {
				logger.Error("WAL file corrupted",
					zap.String("fraction", r.baseFileName),
					zap.Int64("from", corruptionStart),
					zap.Int64("to", offset))
				corruptionStart = -1
			}
		}
		startCorruptionTracking := func(offset int64) {
			if corruptionStart < 0 {
				corruptionStart = offset
			}
		}

		for {
			headerBuf := make([]byte, MetaBlockHeaderLen)
			n, err := r.limiter.ReadAt(r.reader, headerBuf, offset)

			if err != nil && !errors.Is(err, io.EOF) {
				logCorruptionEnd(offset)
				yield(WalRecord{Offset: offset, Err: err})
				return
			}

			if errors.Is(err, io.EOF) || n < MetaBlockHeaderLen {
				logCorruptionEnd(offset)
				return
			}

			if !IsMetaBlock(headerBuf) {
				startCorruptionTracking(offset)
				offset += BlockAlignment
				continue
			}

			mb := MetaBlock(headerBuf)

			if !mb.IsHeaderCorrect() {
				startCorruptionTracking(offset)
				offset += BlockAlignment
				continue
			}

			// header is correct, try to read the payload
			blockLen := int64(mb.FullLen())
			blockBuf := make([]byte, blockLen)
			n, err = r.limiter.ReadAt(r.reader, blockBuf, offset)

			if err != nil && !errors.Is(err, io.EOF) {
				// this is the last WAL record
				// start corruption tracking if not started already and print
				startCorruptionTracking(offset)
				logCorruptionEnd(offset)
				yield(WalRecord{Offset: offset, Err: err})
				return
			}

			if errors.Is(err, io.EOF) || int64(n) < blockLen {
				startCorruptionTracking(offset)
				logCorruptionEnd(offset)
				return
			}

			mb = blockBuf

			if !mb.IsPayloadCorrect() {
				startCorruptionTracking(offset)
				offset = nextBlockOffset(offset + blockLen)
				continue
			}

			logCorruptionEnd(offset)

			entry := WalRecord{
				Data:   mb,
				Offset: offset,
				Size:   blockLen,
			}

			if !yield(entry) {
				return
			}

			offset = nextBlockOffset(offset + blockLen)
		}
	}
}

// nextBlockOffset aligns provided offset to BlockAlignment
func nextBlockOffset(offset int64) int64 {
	return (offset + BlockAlignment - 1) &^ (BlockAlignment - 1)
}
