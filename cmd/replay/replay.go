package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/util"
	"go.uber.org/zap"
)

type cmdReplay struct {
	frac        string
	address     string
	mode        string
	batchSize   int
	mappingPath string
	dryRun      bool
}

func (c *cmdReplay) run(ctx context.Context) error {
	if c.batchSize <= 0 {
		return errors.New("batch-size must be positive")
	}

	sender, err := newSender(c.mode, c.address, c.mappingPath)
	if err != nil {
		return err
	}

	batcher := newBatcher(ctx, c.batchSize, c.dryRun, sender)

	docs, err := openDocs(c.frac + consts.SdocsFileSuffix)
	if errors.Is(err, os.ErrNotExist) {
		docs, err = openDocs(c.frac + consts.DocsFileSuffix)
	}
	if err != nil {
		return err
	}
	defer docs.Close()

	if !c.dryRun {
		batcher.replayPath = replayTempPath(c.frac)
		batcher.sent, err = readReplayOffset(batcher.replayPath)
		if err != nil {
			return err
		}
		if err := writeReplayOffset(batcher.replayPath, batcher.sent); err != nil {
			return err
		}
	}

	logger.Info("started replaying docs")

	for toSkip := batcher.sent; toSkip > 0; toSkip-- {
		doc, err := docs.next()
		if err != nil {
			return err
		}
		if doc == nil {
			return errors.New("replay offset exceeds document count")
		}
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		doc, err := docs.next()
		if err != nil {
			return err
		}
		if doc == nil {
			break
		}

		if err := batcher.add(doc); err != nil {
			return err
		}
	}
	if err := batcher.flush(ctx); err != nil {
		return err
	}
	if !c.dryRun {
		if err := os.Remove(batcher.replayPath); err != nil {
			return fmt.Errorf("cannot remove replay progress: %w", err)
		}
	}

	logger.Info("finished replaying docs", zap.Int("total", batcher.sent))
	return nil
}

func readReplayOffset(path string) (int, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}

	if err != nil {
		return 0, fmt.Errorf("cannot read replay progress: %w", err)
	}

	offset, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("invalid replay offset '%q'", data)
	}

	return offset, nil
}

func writeReplayOffset(path string, offset int) error {
	data := []byte(strconv.Itoa(offset) + "\n")
	if err := util.WriteFileAtomic(path, data, 0o600, ".tmp"); err != nil {
		return fmt.Errorf("cannot write replay progress: %w", err)
	}
	return nil
}

func replayTempPath(frac string) string {
	return filepath.Join(os.TempDir(), filepath.Base(frac)+".replay")
}

// docsReader streams documents out of a .docs or .sdocs file, one block at a time.
// Next follows the same contract as the proxy's readNext: a nil doc with a
// nil error means the end of the file.
type docsReader struct {
	f      *os.File
	reader storage.DocBlocksReader
	size   int64

	offset int64
	block  []byte
}

func openDocs(path string) (*docsReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cannot open docs file: %w", err)
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("cannot stat docs file: %w", err)
	}

	return &docsReader{
		f:      f,
		reader: storage.NewDocBlocksReader(storage.NewReadLimiter(1, nil), f),
		size:   stat.Size(),
	}, nil
}

func (r *docsReader) Close() error {
	return r.f.Close()
}

func (r *docsReader) next() ([]byte, error) {
	for {
		if len(r.block) != 0 {
			if len(r.block) < 4 {
				return nil, fmt.Errorf("corrupted doc block at offset %d: truncated doc length", r.offset)
			}

			l := binary.LittleEndian.Uint32(r.block)
			if uint64(len(r.block)-4) < uint64(l) {
				return nil, fmt.Errorf(
					"corrupted doc block at offset %d: doc length %d exceeds block size %d",
					r.offset, l, len(r.block)-4,
				)
			}

			doc := r.block[4 : l+4]
			r.block = r.block[l+4:]
			return doc, nil
		}

		block, n, err := r.reader.ReadDocBlockPayload(r.offset)
		if errors.Is(err, io.EOF) {
			if r.offset == r.size {
				return nil, nil
			}

			return nil, fmt.Errorf("truncated doc block header at offset %d", r.offset)
		}
		if err != nil {
			return nil, fmt.Errorf("cannot read doc block at offset %d: %w", r.offset, err)
		}
		r.offset += int64(n)
		r.block = block
	}
}
