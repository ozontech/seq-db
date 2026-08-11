package skipmaskmanager

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/RoaringBitmap/roaring/v2"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/logger"
)

type loader struct {
	filePath     string
	headers      []lidsBlockHeader
	file         *os.File
	headersCache *cache.Cache[[]lidsBlockHeader]
	cashKey      uint32
}

func newLoader(filePath string, headersCache *cache.Cache[[]lidsBlockHeader]) *loader {
	return &loader{
		filePath:     filePath,
		headersCache: headersCache,
		cashKey:      hashFilePath(filePath),
	}
}

func (l *loader) getFile() (*os.File, error) {
	if l.file == nil {
		f, err := os.Open(l.filePath)
		if err != nil {
			return nil, err
		}
		l.file = f
	}
	return l.file, nil
}

func (l *loader) getHeaders() ([]lidsBlockHeader, error) {
	return l.headersCache.Get(l.cashKey, l)
}

func (l *loader) Load(uint32) ([]lidsBlockHeader, int, error) {
	headers, err := l.loadHeaders()
	if err != nil {
		return headers, 0, err
	}
	size := len(headers) * int(lidsBlockHeaderSizeBytes)
	return headers, size, nil
}

func (l *loader) loadHeaders() ([]lidsBlockHeader, error) {
	file, err := l.getFile()
	if err != nil {
		return nil, err
	}

	numBuf := make([]byte, 1+4) // block version 1 byte + number of blocks 4 bytes
	n, err := file.ReadAt(numBuf, 0)
	if err != nil {
		return nil, fmt.Errorf("can't read headers from disk: %s", err.Error())
	}
	if n == 0 {
		return nil, fmt.Errorf("can't read headers from disk: n=0")
	}

	version := skipMaskBinVersion(numBuf[0])
	if _, ok := availableVersions[version]; !ok {
		return nil, fmt.Errorf("invalid LIDs binary version: %d", version)
	}

	headersPos := n
	numberOfBlocks := binary.LittleEndian.Uint32(numBuf[1:])
	headersBuf := make([]byte, numberOfBlocks*uint32(lidsBlockHeaderSizeBytes))

	n, err = file.ReadAt(headersBuf, int64(headersPos))
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("can't read headers, %s", err.Error())
	}
	if n != len(headersBuf) {
		return nil, fmt.Errorf("can't read headers, read=%d, requested=%d", n, len(headersBuf))
	}

	headers := make([]lidsBlockHeader, 0, numberOfBlocks)
	for range numberOfBlocks {
		header := lidsBlockHeader{}
		headersBuf, err = header.unmarshal(headersBuf)
		if err != nil {
			return nil, fmt.Errorf("can't unmarshal lids header: %s", err)
		}
		headers = append(headers, header)
	}

	if len(headersBuf) > 0 {
		return nil, fmt.Errorf("unexpected tail when unmarshaling LIDs headers")
	}

	return headers, nil
}

func (l *loader) loadBlock(index int, add func(uint32)) error {
	if err := l.ensureHeaders(); err != nil {
		return err
	}

	if len(l.headers) < index+1 {
		return fmt.Errorf("can't load block: headers len=%d, index=%d", len(l.headers), index)
	}

	file, err := l.getFile()
	if err != nil {
		return err
	}

	header := l.headers[index]

	blockBuf := make([]byte, header.Size)
	n, err := file.ReadAt(blockBuf, int64(header.Offset))
	if err != nil {
		return err
	}
	if n != len(blockBuf) {
		return fmt.Errorf("can't read lids block, read=%d, requested=%d", n, len(blockBuf))
	}

	blockBuf, err = unmarshalLIDsBlock(blockBuf, header, add)
	if err != nil {
		return err
	}

	if len(blockBuf) > 0 {
		return fmt.Errorf("unexpected tail when unmarshaling LIDs block")
	}

	return nil
}

func (l *loader) loadToBitmap(bitmap *roaring.Bitmap, minLID, maxLID uint32) (err error) {
	defer func() {
		err = errors.Join(err, l.release())
	}()

	if err := l.ensureHeaders(); err != nil {
		return err
	}

	for i, header := range l.headers {
		if header.MaxLID < minLID || header.MinLID > maxLID {
			continue
		}

		err := l.loadBlock(i, func(lid uint32) {
			bitmap.Add(lid)
		})
		if err != nil {
			return err
		}
	}

	return
}

func (l *loader) ensureHeaders() error {
	if l.headers == nil {
		headers, err := l.getHeaders()
		if err != nil {
			return err
		}
		l.headers = headers
	}

	return nil
}

func (l *loader) release() error {
	if l.file != nil {
		if err := l.file.Close(); err != nil {
			logger.Error("can't close skip mask file", zap.Error(err))
			return err
		}
		l.file = nil
	}
	return nil
}
