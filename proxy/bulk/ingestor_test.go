package bulk

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/packer"
	"github.com/ozontech/seq-db/seq"
)

func TestProcessDocuments(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cfg := IngestorConfig{
		MaxInflightBulks:       1,
		AllowedTimeDrift:       time.Hour,
		FutureAllowedTimeDrift: time.Hour,
		TokenMapping: map[string]seq.MappingTypes{
			"path":                                  newMapping(seq.TokenizerTypePath),
			"level":                                 newMapping(seq.TokenizerTypeKeyword),
			"message":                               newMapping(seq.TokenizerTypeText),
			"error":                                 newMapping(seq.TokenizerTypeText),
			"shard":                                 newMapping(seq.TokenizerTypeKeyword),
			"trace_id":                              newMapping(seq.TokenizerTypeKeyword),
			"trace_duration":                        newMapping(seq.TokenizerTypeKeyword),
			"service":                               newMapping(seq.TokenizerTypeKeyword),
			"tags":                                  newMapping(seq.TokenizerTypeTags),
			"tags.level":                            newMapping(seq.TokenizerTypeKeyword),
			"tags.message":                          newMapping(seq.TokenizerTypeText),
			"tags.path":                             newMapping(seq.TokenizerTypePath),
			"spans":                                 newMapping(seq.TokenizerTypeNested),
			"spans.trace_id":                        newMapping(seq.TokenizerTypeKeyword),
			"spans.span_id":                         newMapping(seq.TokenizerTypeKeyword),
			"spans.duration":                        newMapping(seq.TokenizerTypeKeyword),
			"spans.tags":                            newMapping(seq.TokenizerTypeTags),
			"spans.tags.zone":                       newMapping(seq.TokenizerTypeKeyword),
			"spans.tags.otel.status_code":           newMapping(seq.TokenizerTypeKeyword),
			"spans.error_tag":                       newMapping(seq.TokenizerTypeKeyword),
			"spans.process":                         newMapping(seq.TokenizerTypeObject),
			"spans.process.service_name":            newMapping(seq.TokenizerTypeKeyword),
			"spans.process.tags":                    newMapping(seq.TokenizerTypeTags),
			"spans.process.tags.app.version":        newMapping(seq.TokenizerTypeKeyword),
			"spans.process.tags.k8s.namespace.name": newMapping(seq.TokenizerTypeKeyword),
			"spans.process.tags.k8s.label.app":      newMapping(seq.TokenizerTypeKeyword),
			"spans.process.tags.k8s.service.name":   newMapping(seq.TokenizerTypeKeyword),
			"spans.process.tags.k8s.pod.name":       newMapping(seq.TokenizerTypeKeyword),
			"spans.process.tags.k8s.node.name":      newMapping(seq.TokenizerTypeKeyword),
			"spans.process.tags.k8s.host.name":      newMapping(seq.TokenizerTypeKeyword),
			"spans.operation_name":                  newMapping(seq.TokenizerTypeKeyword),
			"spans.tagsMap":                         newMapping(seq.TokenizerTypeObject),
			"spans.tagsMap.error":                   newMapping(seq.TokenizerTypeKeyword),
			"spans.tagsMap.ip":                      newMapping(seq.TokenizerTypeKeyword),
			"spans.tagsMap.span.kind":               newMapping(seq.TokenizerTypeKeyword),
			"spans.tagsMap.page.type":               newMapping(seq.TokenizerTypeKeyword),
			"spans.tagsMap.slo.violated":            newMapping(seq.TokenizerTypeKeyword),
			"spans.tagsMap.http.url":                newMapping(seq.TokenizerTypeKeyword),
			"spans.tagsMap.http.method":             newMapping(seq.TokenizerTypeKeyword),
			"spans2":                                newMapping(seq.TokenizerTypeNested),
			"spans2.span_id":                        newMapping(seq.TokenizerTypeKeyword),
			"spans2.operation_name":                 newMapping(seq.TokenizerTypeKeyword)},
		MaxTokenSize:           consts.KB,
		CaseSensitive:          false,
		PartialFieldIndexing:   false,
		DocsZSTDCompressLevel:  -1,
		MetasZSTDCompressLevel: -1,
		MaxDocumentSize:        consts.KB,
	}

	now := time.Now().UTC()

	id := seq.SimpleID(int(now.UnixMilli()))

	type TestPayload struct {
		InDocs  []string
		ExpDocs []string
		ExpMeta []frac.MetaData
	}
	type TestCase struct {
		Name    string
		Payload func() TestPayload
	}

	tests := []TestCase{
		{
			Name: "empty_doc",
			Payload: func() TestPayload {
				return TestPayload{
					InDocs:  []string{"{}"},
					ExpDocs: nil,
					ExpMeta: []frac.MetaData{{
						ID:     id,
						Size:   2,
						Tokens: []frac.MetaToken{newToken(seq.TokenAll, "")},
					}},
				}
			},
		},
		{
			Name: "check_nulls",
			Payload: func() TestPayload {
				doc := fmt.Sprintf(`{"tags": [{"key": "level"}, {"key": "message"}, {"key": "path"}], "timestamp": %q}`, now.Format(time.RFC3339Nano))
				return TestPayload{
					InDocs:  []string{doc},
					ExpDocs: []string{doc},
					ExpMeta: []frac.MetaData{
						{ID: id, Size: uint32(len(doc)), Tokens: []frac.MetaToken{
							newToken(seq.TokenAll, ""),
							newToken("tags.level", ""), // TODO: this token shouldn't be appear.
							newToken("_exists_", "tags.level"),
							newToken("tags.message", ""), // TODO: this token shouldn't be appear.
							newToken("_exists_", "tags.message"),
							newToken("tags.path", ""), // TODO: this token shouldn't be appear.
							newToken("_exists_", "tags.path"),
						}},
					},
				}
			},
		},
		{
			Name: "too_large_document",
			Payload: func() TestPayload {
				doc1 := fmt.Sprintf(`{"level": %q}`, strings.Repeat("a", consts.KB+1))
				doc2 := fmt.Sprintf(`{"message": %q}`, strings.Repeat("a", consts.KB+1))
				doc3 := fmt.Sprintf(`{"path": %q}`, strings.Repeat("a", consts.KB+1))
				return TestPayload{
					InDocs:  []string{doc1, doc2, doc3},
					ExpDocs: nil,
					ExpMeta: []frac.MetaData{
						{ID: id, Size: uint32(len(doc1)), Tokens: []frac.MetaToken{newToken(seq.TokenAll, ""), newToken(seq.TokenExists, "level")}},
						{ID: id, Size: uint32(len(doc2)), Tokens: []frac.MetaToken{newToken(seq.TokenAll, ""), newToken(seq.TokenExists, "message")}},
						{ID: id, Size: uint32(len(doc3)), Tokens: []frac.MetaToken{newToken(seq.TokenAll, ""), newToken(seq.TokenExists, "path")}},
					},
				}
			},
		},
		{
			Name: "simple_document",
			Payload: func() TestPayload {
				const doc = `{"level":"error", "message":" request 🫦 failed! ", "error": "context cancelled", "shard": "1", "path":"http://localhost:8080/example"}`
				meta := frac.MetaData{ID: id, Size: uint32(len(doc)), Tokens: []frac.MetaToken{
					newToken(seq.TokenAll, ""),
					newToken("level", "error"),
					newToken(seq.TokenExists, "level"),
					newToken("message", "request"),
					newToken("message", "failed"),
					newToken(seq.TokenExists, "message"),
					newToken("error", "context"),
					newToken("error", "cancelled"),
					newToken(seq.TokenExists, "error"),
					newToken("shard", "1"),
					newToken(seq.TokenExists, "shard"),
					newToken("path", "http:"),
					newToken("path", "http:/"),
					newToken("path", "http://localhost:8080"),
					newToken("path", "http://localhost:8080/example"),
					newToken(seq.TokenExists, "path"),
				}}

				return TestPayload{
					InDocs:  []string{doc, doc, doc},
					ExpDocs: []string{doc, doc, doc},
					ExpMeta: []frac.MetaData{meta, meta, meta},
				}
			},
		},
		{
			Name: "tracing",
			Payload: func() TestPayload {
				type Tag struct {
					Key   string `json:"key"`
					Value string `json:"value,omitempty"`
					VStr  string `json:"v_str,omitempty"`
					VType int64  `json:"v_type,omitempty"`
				}
				type TagsMap struct {
					Error      string `json:"error"`
					PageType   string `json:"page.type"`
					SpanKind   string `json:"span.kind"`
					HTTPURL    string `json:"http.url"`
					HTTPMethod string `json:"http.method"`
				}
				type Process struct {
					ServiceName string `json:"service_name"`
					Tags        []Tag  `json:"tags"`
				}
				type Reference struct {
					TraceID string `json:"trace_id"`
					SpanID  string `json:"span_id"`
				}
				type Span struct {
					TagsMap       TagsMap     `json:"tagsMap"`
					TraceID       string      `json:"trace_id"`
					SpanID        string      `json:"span_id"`
					OperationName string      `json:"operation_name"`
					References    []Reference `json:"references"`
					Flags         int64       `json:"flags"`
					StartTime     time.Time   `json:"start_time"`
					Duration      int64       `json:"duration"`
					Tags          []Tag       `json:"tags"`
					Logs          interface{} `json:"logs"`
					Process       Process     `json:"process"`
					Time          string      `json:"time"`
				}
				type Trace struct {
					Spans             []Span `json:"spans"`
					TraceID           string `json:"trace_id"`
					TraceDuration     int64  `json:"trace_duration"`
					Timestamp         string `json:"timestamp"`
					Service           string `json:"service"`
					OriginalTimestamp string `json:"original_timestamp,omitempty"`
				}
				trace := Trace{
					Spans: []Span{{
						TagsMap:       TagsMap{Error: "false", PageType: "", SpanKind: "", HTTPURL: "", HTTPMethod: ""},
						TraceID:       "AAAAAAAAAAAbCmADWEwUBQ==",
						SpanID:        "IszfJv0zhwU=",
						OperationName: "memcache: get_multi",
						References:    []Reference{{TraceID: "AAAAAAAAAAAbCmADWEwUBQ==", SpanID: "bS8caqHI6gU="}},
						Flags:         4242,
						StartTime:     time.Date(2024, 9, 24, 14, 10, 14, 2673000, time.UTC),
						Duration:      4000,
						Tags:          []Tag{{Key: "zone", VStr: "z504"}, {Key: "otel.status_code", VType: 2}},
						Logs:          any([]string{`{"time":"2024-09-24T14:10:14.267Z","message": "request failed: context canceled"}`}),
						Process: Process{
							ServiceName: "seq-ui-server",
							Tags: []Tag{
								{Key: "app.version", Value: "release-s3-10455"},
								{Key: "k8s.namespace.name", Value: "s3"},
								{Key: "k8s.label.app", Value: "seq-ui-server"},
								{Key: "k8s.service.name", Value: "seq-ui-server-34405"},
								{Key: "k8s.pod.name", Value: "seq-ui-server-34405-z504-65f977fc86-wml9z"},
								{Key: "k8s.node.name", Value: "10.139.224.142"},
								{Key: "host.name", Value: "seq-ui-server-34405-z504-65f977fc86-wml9z"},
							},
						},
						Time: now.Format(time.RFC3339),
					}},
					TraceID:       "AAAAAAAAAAAbCmADWEwUBQ==",
					TraceDuration: 137252000,
					Timestamp:     now.Add(-cfg.AllowedTimeDrift * 6).Format(consts.ESTimeFormat),
					Service:       "seq-ui",
				}

				inTrace, err := json.Marshal(trace)
				if err != nil {
					panic(err)
				}

				trace.OriginalTimestamp = now.Add(-cfg.AllowedTimeDrift * 6).Truncate(time.Millisecond).Format(time.RFC3339Nano)
				trace.Timestamp = now.Format(time.RFC3339Nano)
				expTrace, err := json.Marshal(trace)
				if err != nil {
					panic(err)
				}

				return TestPayload{
					InDocs:  []string{string(inTrace)},
					ExpDocs: []string{string(expTrace)},
					ExpMeta: []frac.MetaData{
						{ID: id, Size: uint32(len(expTrace)), Tokens: buildKeywordTokens(
							"trace_id", "aaaaaaaaaaabcmadwewubq==",
							"trace_duration", "137252000",
							"service", "seq-ui")},
						{ID: id, Size: 0, Tokens: buildKeywordTokens(
							"spans.tagsMap.error", "false",
							"spans.tagsMap.page.type", "",
							"spans.tagsMap.span.kind", "",
							"spans.tagsMap.http.url", "",
							"spans.tagsMap.http.method", "",
							"spans.trace_id", "aaaaaaaaaaabcmadwewubq==",
							"spans.span_id", "iszfjv0zhwu=",
							"spans.operation_name", "memcache: get_multi",
							"spans.duration", "4000",
							"spans.tags.zone", "",
							"spans.tags.otel.status_code", "",
							"spans.process.service_name", "seq-ui-server",
							"spans.process.tags.app.version", "release-s3-10455",
							"spans.process.tags.k8s.namespace.name", "s3",
							"spans.process.tags.k8s.label.app", "seq-ui-server",
							"spans.process.tags.k8s.service.name", "seq-ui-server-34405",
							"spans.process.tags.k8s.pod.name", "seq-ui-server-34405-z504-65f977fc86-wml9z",
							"spans.process.tags.k8s.node.name", "10.139.224.142",
							"trace_id", "aaaaaaaaaaabcmadwewubq==",
							"trace_duration", "137252000",
							"service", "seq-ui")},
					},
				}
			},
		},
		{
			Name: "multi_nested_fields_and_docs",
			Payload: func() TestPayload {
				type Span struct {
					SpanID string `json:"span_id"`
					OpName string `json:"operation_name"`
				}
				type Nested struct {
					Spans []Span `json:"spans"`
					Array []Span `json:"spans2"`
				}
				doc := mustMarshalJSON(Nested{
					Spans: []Span{
						{SpanID: "1", OpName: "op1"},
						{SpanID: "2", OpName: "op2"},
						{SpanID: "3", OpName: "op3"},
					},
					Array: []Span{
						{SpanID: "4", OpName: "op4"},
						{SpanID: "5", OpName: "op5"},
						{SpanID: "6", OpName: "op6"},
					},
				})

				meta := []frac.MetaData{
					{ID: id, Size: uint32(len(doc)), Tokens: buildKeywordTokens()},
					{ID: id, Size: 0, Tokens: buildKeywordTokens("spans.span_id", "1", "spans.operation_name", "op1")},
					{ID: id, Size: 0, Tokens: buildKeywordTokens("spans.span_id", "2", "spans.operation_name", "op2")},
					{ID: id, Size: 0, Tokens: buildKeywordTokens("spans.span_id", "3", "spans.operation_name", "op3")},
					{ID: id, Size: 0, Tokens: buildKeywordTokens("spans2.span_id", "4", "spans2.operation_name", "op4")},
					{ID: id, Size: 0, Tokens: buildKeywordTokens("spans2.span_id", "5", "spans2.operation_name", "op5")},
					{ID: id, Size: 0, Tokens: buildKeywordTokens("spans2.span_id", "6", "spans2.operation_name", "op6")}}
				return TestPayload{
					InDocs:  []string{doc, doc, doc},
					ExpDocs: []string{doc, doc, doc},
					ExpMeta: slices.Repeat(meta, 3),
				}
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			payload := tc.Payload()

			r := require.New(t)
			c := &FakeClient{}
			ingestor := NewIngestor(cfg, c)
			defer ingestor.Stop()

			inDocs := payload.InDocs

			total, err := ingestor.ProcessDocuments(ctx, now, func() ([]byte, error) {
				if len(inDocs) == 0 {
					return nil, nil
				}
				doc := []byte(inDocs[0])
				inDocs = inDocs[1:]
				return doc, nil
			})

			r.NoError(err)
			r.Equal(len(payload.InDocs), total)
			r.Equal(len(payload.InDocs), c.total)

			_, binaryDocs, err := disk.DecompressDocBlock(c.docs, nil)
			require.NoError(t, err)

			docsUnpacker := packer.NewBytesUnpacker(binaryDocs)
			var gotDocs [][]byte
			for docsUnpacker.Len() > 0 {
				gotDocs = append(gotDocs, docsUnpacker.GetBinary())
			}

			_, binaryMetas, err := disk.DecompressDocBlock(c.metas, nil)
			require.NoError(t, err)
			metasUnpacker := packer.NewBytesUnpacker(binaryMetas)
			var gotMetas []frac.MetaData
			for metasUnpacker.Len() > 0 {
				meta := frac.MetaData{}
				r.NoError(meta.UnmarshalBinary(metasUnpacker.GetBinary()))
				gotMetas = append(gotMetas, meta)
			}

			// Assert docs.
			for i, expDoc := range payload.ExpDocs {
				r.Equal(expDoc, string(gotDocs[i]))
			}
			// Assert metas.
			r.Equal(len(payload.ExpMeta), len(gotMetas))
			for i, expMeta := range payload.ExpMeta {
				gotMetas[i].ID.RID = 0 // Ignore RID because it is random.
				r.Equalf(expMeta, gotMetas[i], "i=%d:\n got:  %v\n want: %v\n", i, gotMetas[i], payload.ExpMeta[i])
			}
		})
	}
}

type FakeClient struct {
	docs  []byte
	metas []byte
	total int
}

func (f *FakeClient) StoreDocuments(_ context.Context, total int, docs, metas []byte) error {
	f.total = total
	f.docs = docs
	f.metas = metas
	return nil
}

func BenchmarkProcessDocuments(b *testing.B) {
	ctx := context.Background()
	cfg := IngestorConfig{
		MaxInflightBulks:       1,
		AllowedTimeDrift:       time.Hour * 100,
		FutureAllowedTimeDrift: time.Hour * 100,
		TokenMapping: map[string]seq.MappingTypes{
			"level":   seq.NewSingleType(seq.TokenizerTypeKeyword, "level", consts.KB),
			"message": seq.NewSingleType(seq.TokenizerTypeText, "message", consts.KB),
			"error":   seq.NewSingleType(seq.TokenizerTypeText, "error", consts.KB),
			"shard":   seq.NewSingleType(seq.TokenizerTypeKeyword, "shard", consts.KB),
		},
		CaseSensitive: false,
		MaxTokenSize:  consts.KB,
	}

	ingestor := NewIngestor(cfg, &FakeClient{})
	defer ingestor.Stop()
	doc := []byte(fmt.Sprintf(`{
		"level":"error",
		"timestamp":%q,
		"message":"can't store in the shard",
		"error":"circuit breaker execute: can't receive bulk acceptance: 
			host=***REMOVED***, err=rpc error: code = Unavailable desc = connection error: 
			desc = \"transport: Error while dialing: dial tcp 10.233.140.20:9002: connect: connection refused\"",
		"shard":0
	}`, time.Now().Format(consts.ESTimeFormat)))

	const docsToProcess = 1000
	now := time.Now()

	b.SetBytes(int64(len(doc) * docsToProcess))

	for i := 0; i < b.N; i++ {
		n := 0
		total, err := ingestor.ProcessDocuments(ctx, now, func() ([]byte, error) {
			n++
			if n-1 == docsToProcess {
				return nil, nil
			}
			return doc, nil
		})
		if err != nil {
			b.Fatal(err)
		}
		if total != docsToProcess {
			b.Fatal("wrong total", total, docsToProcess)
		}
	}
}

func newMapping(mappingType seq.TokenizerType) seq.MappingTypes {
	return seq.NewSingleType(mappingType, "", consts.KB)
}

func newToken(k, v string) frac.MetaToken {
	return frac.MetaToken{
		Key:   []byte(k),
		Value: []byte(v),
	}
}

func buildKeywordTokens(kvs ...string) []frac.MetaToken {
	var tokens []frac.MetaToken

	tokens = append(tokens, newToken("_all_", ""))

	for i := 0; i < len(kvs); i += 2 {
		k := kvs[i]
		v := kvs[i+1]
		tokens = append(tokens, newToken(k, v), newToken(seq.TokenExists, k))
	}
	return tokens
}

func mustMarshalJSON(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(data)
}
