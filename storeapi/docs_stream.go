package storeapi

import (
	"context"

	"github.com/c2h5oh/datasize"
	"github.com/ozontech/seq-db/conf"
	"github.com/ozontech/seq-db/fetcher"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
	"go.uber.org/zap"
)

const initChunkSize = 1000

type streamDocsBatch struct {
	docs [][]byte
	err  error
}

type docsStream struct {
	ctx      context.Context
	ids      seq.IDSources
	totalIDs int
	fetcher  *fetcher.Fetcher
	fracs    frac.List
	out      chan streamDocsBatch
	docsBuf  [][]byte
}

func (d *docsStream) Next() ([]byte, error) {
	if len(d.docsBuf) == 0 {
		var err error
		if d.docsBuf, err = d.nextBatch(); err != nil {
			return nil, err
		}
	}

	doc := d.docsBuf[0]
	d.docsBuf = d.docsBuf[1:]

	return doc, nil
}

func (d *docsStream) nextBatch() ([][]byte, error) {
	b, ok := <-d.out
	if !ok {
		if d.ctx.Err() != nil { // streaming was interrupted by the client or due to a timeout
			return nil, d.ctx.Err()
		}
		panic("no more docs to fetch; Next() was called either more times than the number of ids, or after an error")
	}
	if b.err != nil {
		return nil, b.err
	}
	return b.docs, nil
}

func (d *docsStream) batchLoader() {
	chunkSize := initChunkSize

	for len(d.ids) > 0 {
		select {
		case <-d.ctx.Done():
			return
		default:
			// cut chunk
			l := min(len(d.ids), chunkSize)
			chunk := d.ids[:l]
			d.ids = d.ids[l:]

			// fetch chunk
			docs, err := d.fetcher.FetchDocs(d.ctx, d.fracs, chunk)

			// first we send regardless of the error,
			// the possible error will be handled at the destination.
			select {
			case <-d.ctx.Done():
				return
			case d.out <- streamDocsBatch{docs: docs, err: err}:
			}

			if err != nil { // but stop fetching on error
				return
			}

			chunkSize = d.calcChunkSize(docs, chunkSize)
		}
	}
}

func (d *docsStream) calcChunkSize(docs [][]byte, prevChunkSize int) int {
	batchSize := 0
	for _, doc := range docs {
		batchSize += len(doc)
	}
	if batchSize == 0 {
		return prevChunkSize
	}

	avgDocSize := batchSize / len(docs)
	newChunkSize := conf.MaxFetchSizeBytes / avgDocSize
	if newChunkSize != prevChunkSize {
		logger.Debug(
			"fetch chunk recalculated",
			zap.Int("total_len", d.totalIDs),
			zap.Int("prev_chunk_size", prevChunkSize),
			zap.Int("new_chunk_size", newChunkSize),
			zap.String("batch_size", datasize.ByteSize(batchSize).HumanReadable()),
		)
	}

	return newChunkSize
}

func newDocsStream(ctx context.Context, ids seq.IDSources, fetcher *fetcher.Fetcher, fracs frac.List) *docsStream {
	d := docsStream{
		ctx:      ctx,
		ids:      ids,
		totalIDs: len(ids),
		fetcher:  fetcher,
		fracs:    fracs,
		out:      make(chan streamDocsBatch, 1), // buffered chan to async load the next batch while we are sending the prev batch to the client
	}

	go func() {
		d.batchLoader()
		close(d.out)
	}()

	return &d
}
