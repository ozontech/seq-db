package docsfilter

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

type loader struct {
	headers []lidsBlockHeader
	file    *os.File
	// TODO: seems like cache needs to be populated somewhere outside of this struct and passed here
	// cache *cache.Cache[[]lidsBlockHeader]
}

func newLoader(filePath string) (*loader, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	return &loader{
		file: f,
	}, nil
}

func (l *loader) loadHeaders() error {
	numBuf := make([]byte, 1+4) // block version 1 byte + number of blocks 4 bytes
	n, err := l.file.ReadAt(numBuf, 0)
	if err != nil {
		return fmt.Errorf("can't read headers from disk: %s", err.Error())
	}
	if n == 0 {
		return fmt.Errorf("can't read headers from disk: n=0")
	}

	version := docsFilterBinVersion(numBuf[0])
	if _, ok := availableVersions[version]; !ok {
		return fmt.Errorf("invalid LIDs binary version: %d", version)
	}

	headersPos := n
	numberOfBlocks := binary.BigEndian.Uint32(numBuf[1:])
	headersBuf := make([]byte, numberOfBlocks*uint32(lidsBlockHeaderSizeBytes))

	n, err = l.file.ReadAt(headersBuf, int64(headersPos))
	if err != nil && err != io.EOF {
		return fmt.Errorf("can't read headers, %s", err.Error())
	}
	if n != len(headersBuf) {
		return fmt.Errorf("can't read headers, read=%d, requested=%d", n, len(headersBuf))
	}
	if len(headersBuf)%int(lidsBlockHeaderSizeBytes) != 0 {
		return fmt.Errorf("wrong headers format")
	}

	l.headers = make([]lidsBlockHeader, 0, numberOfBlocks)
	for range numberOfBlocks {
		header := lidsBlockHeader{}
		headersBuf, err = header.unmarshal(headersBuf)
		if err != nil {
			return fmt.Errorf("can't unmarshal lids header: %s", err)
		}
		l.headers = append(l.headers, header)
	}

	if len(headersBuf) > 0 {
		return fmt.Errorf("unexpected tail when unmarshaling LIDs headers")
	}

	return nil
}

func (l *loader) loadBlock(index int) ([]uint32, error) {
	if l.headers == nil {
		err := l.loadHeaders()
		if err != nil {
			return nil, err
		}
	}

	if len(l.headers) < index+1 {
		return nil, fmt.Errorf("can't load block: headers len=%d, index=%d", len(l.headers), index)
	}

	header := l.headers[index]

	blockBuf := make([]byte, header.Size) // TODO: buffer pool (???)
	n, err := l.file.ReadAt(blockBuf, int64(header.Offset))
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
	if err := l.file.Close(); err != nil {
		logger.Error("can't close tombstones file", zap.Error(err))
		return err
	}
	return nil
}
