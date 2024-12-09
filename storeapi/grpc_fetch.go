package storeapi

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"
	"go.opencensus.io/trace"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/conf"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tracing"
	"github.com/ozontech/seq-db/util"
)

func (g *GrpcV1) Fetch(req *storeapi.FetchRequest, stream storeapi.StoreApi_FetchServer) error {
	ctx, span := tracing.StartSpan(stream.Context(), "store-server/Fetch")
	defer span.End()

	if span.IsRecordingEvents() {
		span.AddAttributes(trace.StringAttribute("ids", strings.Join(req.Ids, ",")))
		span.AddAttributes(trace.Int64Attribute("number_of_ids", int64(len(req.Ids))))
		span.AddAttributes(trace.BoolAttribute("explain", req.Explain))
	}

	err := g.doFetch(ctx, req, stream)
	if err != nil {
		span.SetStatus(trace.Status{Code: 1, Message: err.Error()})
		logger.Error("fetch error", zap.Error(err))
	}
	return err
}

func (g *GrpcV1) doFetch(ctx context.Context, req *storeapi.FetchRequest, stream storeapi.StoreApi_FetchServer) error {
	const initChunkSize = 1000

	metric.FetchInFlightQueriesTotal.Inc()
	defer metric.FetchInFlightQueriesTotal.Dec()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	start := time.Now()

	ids, err := extractIDs(req)
	if err != nil {
		return fmt.Errorf("ids extract errors: %s", err.Error())
	}

	notFound := 0
	docsFetched := 0
	bytesFetched := int64(0)

	workDuration := time.Duration(0)
	sendDuration := time.Duration(0)

	var buf []byte
	docsStream := g.docsStream(ctx, ids, initChunkSize)

	sent := 0
	for _, id := range ids {
		workTime := time.Now()
		doc, err := docsStream()
		if err != nil {
			return err
		}
		workDuration += time.Since(workTime)

		if doc == nil {
			notFound++
			logger.Info("doc not found", zap.String("id", id.ID.String()))
		} else {
			docsFetched++
			bytesFetched += int64(len(doc))
		}

		sendTime := time.Now()

		buf = util.EnsureSliceSize(buf, disk.DocBlockHeaderLen+len(doc))
		block := disk.PackDocBlock(doc, buf)
		block.SetExt1(uint64(id.ID.MID))
		block.SetExt2(uint64(id.ID.RID))

		if err := stream.Send(&storeapi.BinaryData{Data: block}); err != nil {
			if util.IsCancelled(ctx) {
				logger.Info("fetch request is canceled",
					zap.Int("requested", len(ids)),
					zap.Int("sent", sent),
					zap.Duration("after", time.Since(start)),
				)
				break
			}
			return fmt.Errorf("error sending fetched docs: %w", err)
		}
		sent++
		sendDuration += time.Since(sendTime)
	}

	overallDuration := time.Since(start)

	metric.FetchDurationSeconds.Observe(float64(overallDuration) / float64(time.Second))
	metric.FetchDocsTotal.Observe(float64(docsFetched))
	metric.FetchDocsNotFound.Observe(float64(notFound))
	metric.FetchBytesTotal.Observe(float64(bytesFetched))

	if g.config.Fetch.LogThreshold != 0 && overallDuration >= g.config.Fetch.LogThreshold {
		logger.Warn("slow fetch",
			zap.Int64("took_ms", overallDuration.Milliseconds()),
			zap.Int("count", len(ids)),
			zap.Int64("work_duration_ms", workDuration.Milliseconds()),
			zap.Int64("send_duration_ms", sendDuration.Milliseconds()),
		)
	}

	if req.Explain {
		logger.Info("fetch result",
			zap.Int("requested", len(ids)),
			zap.Int("fetched", docsFetched),
			util.ZapDurationWithPrec("work_ms", workDuration, "ms", 2),
			util.ZapDurationWithPrec("send_ms", sendDuration, "ms", 2),
			util.ZapUint64AsSizeStr("bytes", uint64(bytesFetched)),
			util.ZapDurationWithPrec("overall_ms", overallDuration, "ms", 2),
		)
	}

	return nil
}

func (g *GrpcV1) docsStream(ctx context.Context, ids []seq.IDSource, initChunkLen int) func() ([]byte, error) {
	var docs [][]byte

	fetchSize := 0
	chunkLen := initChunkLen
	fracs := g.fracManager.GetAllFracs()

	total := len(ids)

	return func() ([]byte, error) {
		if len(docs) == 0 {
			if len(ids) == 0 {
				return nil, errors.New("no more ids for fetch")
			}

			if fetchSize > 0 {
				avgDocSize := fetchSize / chunkLen
				chunkLen = conf.MaxFetchSizeBytes / avgDocSize
				logger.Debug(
					"fetch chunk recalculated",
					zap.Int("total_len", total),
					zap.Int("new_chunk_len", chunkLen),
					zap.String("prev_chunk_size", datasize.ByteSize(fetchSize).HumanReadable()),
				)
				fetchSize = 0
			}

			var (
				err   error
				chunk []seq.IDSource
			)
			l := min(len(ids), chunkLen)
			chunk, ids = ids[:l], ids[l:]
			if docs, err = g.fetchData.docFetcher.FetchDocs(ctx, fracs, chunk); err != nil {
				return nil, err
			}
		}

		var doc []byte
		doc, docs = docs[0], docs[1:]
		fetchSize += len(doc)

		return doc, nil
	}
}

func extractIDsNoHints(idsStr []string) ([]seq.IDSource, error) {
	count := len(idsStr)
	ids := make([]seq.IDSource, 0, count)
	for _, id := range idsStr {
		idStr, err := seq.FromString(id)
		if err != nil {
			return nil, fmt.Errorf("wrong doc id %s format: %s", idStr, err.Error())
		}
		ids = append(ids, seq.IDSource{ID: idStr})
	}
	return ids, nil
}

func extractIDsWithHints(idsReq []*storeapi.IdWithHint) ([]seq.IDSource, error) {
	count := len(idsReq)
	ids := make([]seq.IDSource, 0, count)
	for _, id := range idsReq {
		idStr, err := seq.FromString(id.Id)
		if err != nil {
			return nil, fmt.Errorf("wrong doc id %s format: %s", idStr, err.Error())
		}
		ids = append(ids, seq.IDSource{ID: idStr, Hint: id.Hint})
	}
	return ids, nil
}

func extractIDs(req *storeapi.FetchRequest) ([]seq.IDSource, error) {
	if len(req.IdsWithHints) != 0 {
		return extractIDsWithHints(req.IdsWithHints)
	}
	return extractIDsNoHints(req.Ids)
}
