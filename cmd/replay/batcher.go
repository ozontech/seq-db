package main

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/logger"
)

const maxBulkTries = 10

type bulkSender interface {
	Send(ctx context.Context, docs [][]byte) error
}

func newSender(mode, address, mappingPath string) (bulkSender, error) {
	switch mode {
	case modeProxy:
		return newProxySender(address), nil
	case modeStore:
		if mappingPath == "" {
			return nil, errors.New("store mode requires --mapping: the store does not tokenize documents, it needs pre-built metas")
		}

		mapping, err := loadMapping(mappingPath)
		if err != nil {
			return nil, err
		}

		s, err := newStoreSender(address, mapping)
		if err != nil {
			return nil, err
		}

		return s, nil
	default:
		return nil, fmt.Errorf("unknown mode %q", mode)
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

	replayPath string
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
	if b.replayPath != "" {
		if err := writeReplayOffset(b.replayPath, b.sent); err != nil {
			return err
		}
	}

	logger.Info("sent batch", zap.Int("total", b.sent))

	return nil
}
