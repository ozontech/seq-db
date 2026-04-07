package filtermanager

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

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

func newLoader(filePath string, headersCache *cache.Cache[[]lidsBlockHeader]) (*loader, error) {
	return &loader{
		filePath:     filePath,
		headersCache: headersCache,
		cashKey:      hashFilePath(filePath),
	}, nil
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
	return l.headersCache.GetWithError(l.cashKey, func() ([]lidsBlockHeader, int, error) {
		headers, err := l.loadHeaders()
		if err != nil {
			return headers, 0, err
		}
		size := len(headers) * int(lidsBlockHeaderSizeBytes)
		return headers, size, nil
	})
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

	version := docsFilterBinVersion(numBuf[0])
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

func (l *loader) loadBlock(index int) ([]uint32, error) {
	if l.headers == nil {
		headers, err := l.getHeaders()
		if err != nil {
			return nil, err
		}
		l.headers = headers
	}

	if len(l.headers) < index+1 {
		return nil, fmt.Errorf("can't load block: headers len=%d, index=%d", len(l.headers), index)
	}

	file, err := l.getFile()
	if err != nil {
		return nil, err
	}

	header := l.headers[index]

	blockBuf := make([]byte, header.Size)
	n, err := file.ReadAt(blockBuf, int64(header.Offset))
	if err != nil {
		return nil, err
	}
	if n != len(blockBuf) {
		return nil, fmt.Errorf("can't read lids block, read=%d, requested=%d", n, len(blockBuf))
	}

	lids := make([]uint32, 0, header.Length)
	lids, blockBuf, err = unmarshalLIDsBlock(lids, blockBuf, header)
	if err != nil {
		return nil, err
	}

	if len(blockBuf) > 0 {
		return nil, fmt.Errorf("unexpected tail when unmarshaling LIDs block")
	}

	return lids, nil
}

func (l *loader) release() error {
	if l.file != nil {
		if err := l.file.Close(); err != nil {
			logger.Error("can't close filter file", zap.Error(err))
			return err
		}
		l.file = nil
	}
	return nil
}
