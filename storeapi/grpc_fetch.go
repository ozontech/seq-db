package storeapi

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	insaneJSON "github.com/ozontech/insane-json"
	"go.opencensus.io/trace"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
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

	var (
		sent int
		buf  []byte
	)

	dp := acquireDocFieldsFilter(req.FieldsFilter)
	defer releaseDocFieldsFilter(dp)

	docsStream := newDocsStream(ctx, ids, g.fetchData.docFetcher, g.fracManager.GetAllFracs())
	for _, id := range ids {
		workTime := time.Now()
		doc, err := docsStream.Next()
		if err != nil {
			return err
		}
		doc = dp.FilterDocFields(doc)
		workDuration += time.Since(workTime)

		if doc == nil {
			notFound++
			logger.Info("doc not found", zap.String("id", id.ID.String()))
		} else {
			docsFetched++
			bytesFetched += int64(len(doc))
		}

		sendTime := time.Now()

		buf = util.EnsureSliceSize(buf, storage.DocBlockHeaderLen+len(doc))
		block := storage.PackDocBlock(doc, buf)
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

type docFieldsFilter struct {
	filter *storeapi.FetchRequest_FieldsFilter

	decoder    *insaneJSON.Root
	decoderBuf []byte
}

var docFieldsFilterPool = sync.Pool{
	New: func() any {
		return &docFieldsFilter{}
	},
}

func acquireDocFieldsFilter(filter *storeapi.FetchRequest_FieldsFilter) *docFieldsFilter {
	dp := docFieldsFilterPool.Get().(*docFieldsFilter)
	if dp.decoder == nil {
		dp.decoder = insaneJSON.Spawn()
	}
	dp.filter = filter
	return dp
}

func releaseDocFieldsFilter(dp *docFieldsFilter) {
	dp.filter = nil
	docFieldsFilterPool.Put(dp)
}

// FilterDocFields filters document with dp.filter.
// This function doesn't mutate the given doc slice.
func (dp *docFieldsFilter) FilterDocFields(doc []byte) storage.DocBlock {
	doc = dp.filterFields(doc)
	return doc
}

func (dp *docFieldsFilter) filterFields(doc []byte) []byte {
	if len(dp.filter.GetFields()) == 0 || len(doc) == 0 {
		return doc
	}
	err := dp.decoder.DecodeBytes(doc)
	if err != nil {
		logger.Error("error decoding doc while fetch", zap.Error(err))
		return doc
	}

	if !dp.decoder.IsObject() {
		logger.Error("document is not an object", zap.String("doc", string(doc)))
		return doc
	}

	if !dp.filter.AllowList {
		// It is block list, so remove given fields from document.

		for _, field := range dp.filter.Fields {
			dp.decoder.Dig(field).Suicide()
		}
		dp.decoderBuf = dp.decoder.Encode(dp.decoderBuf[:0])
		return dp.decoderBuf
	}

	// Keep only given fields.

	// fieldsToRemove contains fields that should be removed.
	// It is complex to do it in-place because decoder.Suicide makes decoder.AsFields() invalid.
	var fieldsToRemove []*insaneJSON.Node
	for _, field := range dp.decoder.AsFields() {
		fieldName := field.AsString()
		if !slices.Contains(dp.filter.Fields, fieldName) {
			fieldsToRemove = append(fieldsToRemove, field.AsFieldValue())
		}
	}
	for _, field := range fieldsToRemove {
		field.Suicide()
	}
	dp.decoderBuf = dp.decoder.Encode(dp.decoderBuf[:0])
	return dp.decoderBuf
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
