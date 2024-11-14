package frac

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
)

type frac struct {
	statsMu sync.Mutex

	DocBlocks *UInt64s

	docsFile   *os.File
	docsFileMu sync.RWMutex

	reader *disk.Reader
	info   *Info

	BaseFileName string

	docBlockCache *cache.Cache[[]byte]

	useLock  sync.RWMutex
	suicided bool
}

func (f *frac) Contains(id seq.MID) bool {
	return f.IsIntersecting(id, id)
}

func (f *frac) readDoc(blockPos, blockLen, docPos uint64, outBuf []byte) ([]byte, []byte, error) {
	f.tryOpenDocsFile()

	block, err := f.docBlockCache.GetWithError(uint32(blockPos), func() ([]byte, int, error) {
		readTask := f.reader.ReadDocBlock(f.docsFile, int64(blockPos), blockLen, outBuf)
		outBuf = readTask.Buf
		if readTask.Err == nil {
			readTask.Decompress()
		}
		if readTask.Err != nil {
			return nil, 0, fmt.Errorf("can't fetch doc at pos %d: %s", blockPos, readTask.Err.Error())
		}
		return readTask.Buf, cap(readTask.Buf), nil
	})

	if err != nil {
		return nil, outBuf, err
	}

	docSize := binary.LittleEndian.Uint32(block[docPos:])
	doc := block[4+docPos : 4+docPos+uint64(docSize)]

	return doc, outBuf, nil
}

func (f *frac) IsIntersecting(from, to seq.MID) bool {
	info := f.Info()
	if info.DocsTotal == 0 { // don't include fresh active fraction
		return false
	}

	if to < info.From || info.To < from {
		return false
	}

	if info.Distribution == nil { // can't check distribution
		return true
	}

	// check with distribution
	return info.Distribution.IsIntersecting(from, to)
}

func (f *frac) Info() *Info {
	f.statsMu.Lock()
	defer f.statsMu.Unlock()
	info := *f.info

	return &info
}

func (f *frac) setInfoSealingTime(newTime uint64) {
	f.statsMu.Lock()
	defer f.statsMu.Unlock()

	f.info.SealingTime = newTime
}

func (f *frac) setInfoIndexOnDisk(newSize uint64) {
	f.statsMu.Lock()
	defer f.statsMu.Unlock()

	f.info.IndexOnDisk = newSize
}

func (f *frac) tryOpenDocsFile() {
	f.docsFileMu.Lock()
	defer f.docsFileMu.Unlock()

	if f.docsFile != nil {
		return
	}

	filename := f.BaseFileName + consts.DocsFileSuffix
	docsFile, err := os.Open(filename)
	if err != nil {
		// give hdd a second chance
		docsFile, err = os.Open(filename)
		if err != nil {
			logger.Fatal("can't open docs file",
				zap.String("file", filename),
				zap.Error(err),
			)
		}
	}

	f.docsFile = docsFile
}

func (f *frac) toString(fracType string) string {
	stats := f.Info()
	s := fmt.Sprintf(
		"%s fraction name=%s, creation time=%s, from=%s, to=%s, %s",
		fracType,
		stats.Name(),
		time.UnixMilli(int64(stats.CreationTime)).Format(consts.ESTimeFormat),
		stats.From,
		stats.To,
		stats.String(),
	)
	if fracType == "" {
		return s[1:]
	}
	return s
}

// logArgs returns slice of zap.Field for frac close log.
func (f *frac) closeLogArgs(fracType, hint string, err error) []zap.Field {
	return []zap.Field{
		zap.String("frac", f.BaseFileName),
		zap.String("type", fracType),
		zap.String("hint", hint),
		zap.Error(err),
	}
}
