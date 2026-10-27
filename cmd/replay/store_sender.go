package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/proxy/bulk"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tokenizer"
)

// storeSender builds metas locally (the store does not tokenize documents)
// and sends binary bulks over gRPC.
type storeSender struct {
	client storeapi.StoreApiClient
	proc   *indexer.Processor
	comp   *indexer.DocsMetasCompressor

	docsBuf  []byte
	metasBuf []byte
}

func newStoreSender(address string, mapping seq.Mapping) (*storeSender, error) {
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("cannot create grpc client: %w", err)
	}

	return newStoreSenderWithClient(storeapi.NewStoreApiClient(conn), mapping), nil
}

func newStoreSenderWithClient(client storeapi.StoreApiClient, mapping seq.Mapping) *storeSender {
	tokenizers := map[seq.TokenizerType]tokenizer.Tokenizer{
		seq.TokenizerTypeText:    tokenizer.NewTextTokenizer(consts.DefaultMaxTokenSize, false, true, consts.MaxTextFieldValueLength),
		seq.TokenizerTypeKeyword: tokenizer.NewKeywordTokenizer(consts.DefaultMaxTokenSize, false, true),
		seq.TokenizerTypePath:    tokenizer.NewPathTokenizer(consts.DefaultMaxTokenSize, false, true),
		seq.TokenizerTypeExists:  tokenizer.NewExistsTokenizer(),
	}

	return &storeSender{
		client: client,
		proc:   indexer.NewProcessor(mapping, tokenizers, 0, 0, 0),
		comp:   indexer.GetDocsMetasCompressor(1, 1),
	}
}

func (s *storeSender) Send(ctx context.Context, docsList [][]byte) error {
	requestTime := time.Now()

	s.docsBuf = s.docsBuf[:0]
	s.metasBuf = s.metasBuf[:0]

	total, docsBuf, metasBuf, err := s.proc.ProcessBulk(requestTime, s.docsBuf, s.metasBuf, listReader(docsList))
	if err != nil {
		return fmt.Errorf("cannot process documents: %w", err)
	}
	s.docsBuf, s.metasBuf = docsBuf, metasBuf

	s.comp.CompressDocsAndMetas(s.docsBuf, s.metasBuf)
	docs, metas := s.comp.DocsMetas()

	req := &storeapi.BulkRequest{
		Count: int64(total),
		Docs:  docs,
		Metas: metas,
	}

	var lastErr error
	for attempt := 0; attempt < maxBulkTries; attempt++ {
		_, err := s.client.Bulk(ctx, req,
			grpc.MaxCallRecvMsgSize(bulk.MaxCallRecvMsgSize),
			grpc.MaxCallSendMsgSize(bulk.MaxCallSendMsgSize))
		if err == nil {
			return nil
		}
		lastErr = err
		logger.Warn(
			"store bulk request failed, retrying",
			zap.Int("attempt", attempt+1),
			zap.Int("docs", len(docsList)),
			zap.Error(err),
		)

		if err := sleep(ctx, backoffDelay(attempt)); err != nil {
			return ctx.Err()
		}
	}

	return fmt.Errorf("giving up after %d attempts: %w", maxBulkTries, lastErr)
}

func (s *storeSender) close() {}

// listReader returns a reader function over a slice of documents.
func listReader(docs [][]byte) func() ([]byte, error) {
	i := 0
	return func() ([]byte, error) {
		if i >= len(docs) {
			return nil, nil
		}
		doc := docs[i]
		i++
		return doc, nil
	}
}

func loadMapping(path string) (seq.Mapping, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read mapping file: %w", err)
	}
	return seq.ReadMapping(data)
}
