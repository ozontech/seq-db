package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/storage"
)

const (
	modeProxy = "proxy"
	modeStore = "store"
)

const maxBulkTries = 10

// bulkSender sends one batch of documents to a proxy or a store.
type bulkSender interface {
	Send(ctx context.Context, docs [][]byte) error
}

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

	docsPath := c.frac + ".docs"

	sender, closer, err := newSender(c.mode, c.address, c.mappingPath)
	if err != nil {
		return err
	}
	if closer != nil {
		defer closer()
	}

	batcher := newBatcher(ctx, c.batchSize, c.dryRun, sender)

	docs, err := openDocs(docsPath)
	if err != nil {
		return err
	}
	defer docs.Close()

	for {
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

	logger.Info("finished replaying docs", zap.Int("docs_count", batcher.sent))
	return nil
}

func newSender(mode, address, mappingPath string) (bulkSender, func(), error) {
	switch mode {
	case modeProxy:
		return newProxySender(address), nil, nil
	case modeStore:
		if mappingPath == "" {
			return nil, nil, errors.New("store mode requires --mapping: the store does not tokenize documents, it needs pre-built metas")
		}

		mapping, err := loadMapping(mappingPath)
		if err != nil {
			return nil, nil, err
		}

		s, err := newStoreSender(address, mapping)
		if err != nil {
			return nil, nil, err
		}

		return s, s.close, nil
	default:
		return nil, nil, fmt.Errorf("unknown mode %q", mode)
	}
}

// batcher accumulates documents and sends them to the sender in bulks.
type batcher struct {
	ctx       context.Context
	batchSize int
	dryRun    bool
	sender    bulkSender

	batch [][]byte
	sent  int
}

func newBatcher(ctx context.Context, batchSize int, dryRun bool, sender bulkSender) *batcher {
	return &batcher{
		ctx:       ctx,
		batchSize: batchSize,
		dryRun:    dryRun,
		sender:    sender,
		batch:     make([][]byte, 0, batchSize),
	}
}

func (b *batcher) add(doc []byte) error {
	if b.dryRun {
		b.sent++
		return nil
	}

	b.batch = append(b.batch, doc)
	if len(b.batch) < b.batchSize {
		return nil
	}

	return b.flush(b.ctx)
}

func (b *batcher) flush(ctx context.Context) error {
	if len(b.batch) == 0 {
		return nil
	}

	bctx, cancel := context.WithTimeout(ctx, consts.BulkTimeout)
	defer cancel()

	if err := b.sender.Send(bctx, b.batch); err != nil {
		return err
	}

	b.sent += len(b.batch)
	b.batch = b.batch[:0]

	return nil
}

// docsReader streams documents out of a .docs file, one block at a time.
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
			// A truncated block also reports io.EOF, distinguish it from the
			// honest end of the file by the offset.
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

func backoffDelay(attempt int) time.Duration {
	return time.Duration(attempt) * 100 * time.Millisecond
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

func main() {
	app := kingpin.New("replay", "re-ingests documents from a fraction's .docs file into seq-db")

	replayCmd := &cmdReplay{}
	app.Arg("frac", "base file name of a fraction (without file suffix)").Required().StringVar(&replayCmd.frac)
	app.Flag("mode", "what to send documents to, proxy accepts ES bulk over HTTP, store accepts binary bulks over gRPC").
		Default(modeProxy).EnumVar(&replayCmd.mode, modeProxy, modeStore)
	app.Flag("address", "address of the proxy HTTP endpoint or the store gRPC endpoint").
		Default("http://localhost:9002").StringVar(&replayCmd.address)
	app.Flag("batch-size", "number of documents per bulk request").Default("1000").IntVar(&replayCmd.batchSize)
	app.Flag("mapping", "path to mapping YAML, required in store mode to build metas").ExistingFileVar(&replayCmd.mappingPath)
	app.Flag("dry-run", "only read the docs file and count documents without sending").BoolVar(&replayCmd.dryRun)

	kingpin.MustParse(app.Parse(os.Args[1:]))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := replayCmd.run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(2)
	}
}
