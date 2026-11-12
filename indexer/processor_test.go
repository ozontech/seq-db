package indexer

import (
	"testing"
	"time"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tokenizer"
)

func TestExtractDocTime(t *testing.T) {
	type testCase struct {
		title         string
		input         []byte
		expectedTime  time.Time
		expectedField []string
	}

	// time.Parse uses time.Local if possible
	// so return time.Local if offset equals to local offset
	newFixedZone := func(offset int) *time.Location {
		_, localOffset := time.Now().Local().Zone()
		if offset == localOffset {
			return time.Local
		}

		return time.FixedZone("", offset)
	}

	testCases := []testCase{
		{
			title:         "ESTimeFormat, ts field",
			input:         []byte(`{"message": "hello world", "ts": "2024-04-19 18:04:25.999"}`),
			expectedTime:  time.Date(2024, 4, 19, 18, 4, 25, 999000000, time.UTC),
			expectedField: []string{"ts"},
		},
		{
			title:         "time.RFC3339Nano, time field",
			input:         []byte(`{"message": "hello world", "time": "2024-04-19T18:04:25.999999999+03:00"}`),
			expectedTime:  time.Date(2024, 4, 19, 18, 4, 25, 999999999, newFixedZone(3*60*60)),
			expectedField: []string{"time"},
		},
		{
			title:         "time.RFC3339, timestamp field",
			input:         []byte(`{"message": "hello world", "timestamp": "2024-04-19T18:04:25+03:00"}`),
			expectedTime:  time.Date(2024, 4, 19, 18, 4, 25, 0, newFixedZone(3*60*60)),
			expectedField: []string{"timestamp"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			root := insaneJSON.Spawn()
			defer insaneJSON.Release(root)

			require.NoError(t, root.DecodeBytes(tc.input))

			docTime, timeField := extractDocTime(root.Node, time.Now())
			assert.Equal(t, tc.expectedTime, docTime)
			assert.Equal(t, tc.expectedField, timeField)
		})
	}
}

func TestExtractDocTimeUnknownTimeFormat(t *testing.T) {
	inputs := [][]byte{
		[]byte(`{"message": "hello world 1", "time": "2024-04-19T17:08:21.203+0500"}`),
		[]byte(`{"message": "hello world 2", "ts": "2024-04-19T17:08:21.203+0500"}`),
		[]byte(`{"message": "hello world 3", "timestamp": "2024-04-19T17:08:21.203+0500"}`),
	}

	root := insaneJSON.Spawn()
	defer insaneJSON.Release(root)

	for _, input := range inputs {
		assert.NoError(t, root.DecodeBytes(input))

		docTime, timeField := extractDocTime(root.Node, time.Now())
		assert.Nil(t, timeField)
		assert.NotEqual(t, 1, docTime.Year())
	}
}

func BenchmarkParseESTime(b *testing.B) {
	const toParse = "2024-04-19 18:04:25.999"
	const toParseRFC3339 = "2024-04-19T18:04:25.999Z"

	b.Run("es_stdlib", func(b *testing.B) {
		for b.Loop() {
			_, err := time.Parse(consts.ESTimeFormat, toParse)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("handwritten", func(b *testing.B) {
		for b.Loop() {
			_, ok := parseESTime(toParse)
			if !ok {
				b.Fatal()
			}
		}
	})

	b.Run("rfc3339", func(b *testing.B) {
		for b.Loop() {
			_, err := time.Parse(time.RFC3339, toParseRFC3339)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestParseESTimePositive(t *testing.T) {
	t.Parallel()

	test := func(input string) {
		t.Helper()

		expected, err := time.Parse(consts.ESTimeFormat, input)
		require.NoError(t, err)

		actual, ok := parseESTime(input)
		require.True(t, ok)
		require.Equal(t, expected, actual)
	}

	test(time.Now().Format(consts.ESTimeFormat))
	test("2024-04-19 18:04:25.999")
	test("2000-01-01 00:00:00.000")
	test("2000-01-01 00:00:00.000000")
	test("2000-01-01 00:00:00.000000000")
	test("2000-01-01 00:00:00")
	test("0000-01-01 00:00:00.000")
	test(consts.ESTimeFormat)
}

func TestParseESTimeNegative(t *testing.T) {
	t.Parallel()

	test := func(input string) {
		t.Helper()

		t1, err := time.Parse(consts.ESTimeFormat, input)
		require.Error(t, err)

		t2, ok := parseESTime(input)
		require.False(t, ok)
		require.Equal(t, t1, t2)
	}

	// Do not pass RFC3339 time.
	test("2024-04-19T18:04:25.999")
	// Handle missing nanoseconds.
	test("2000-01-01 00:00:00.")
	// Handle invalid characters.
	test("0100-01-01 00:00:00.abc")
	test("0A00-01-01 00:00:00.000")
	test("2000-01-01 00:00:AA.000")
	test("0000-01-01T00:00:.0+00:00")
	test("2006-01-02T15:04:05Z_abc")
	test("2007-01-01T00:00:00+00:+0")
	test("2007-01-01T00:00:00+-0:00")
	// Test month/day/year/hour/minute/second is out of range.
	test("2000-00-11 00:00:00.000")
	test("2000-12-07 00:000:00.000")
	test("2000-10-09 000:00:00.000")
	test("2000-09-08 00:00:000.000")
	test("2000-08-001 00:00:00.000")
	test("2000-001-01 00:00:00.000")
	test("20000-01-01 00:00:00.000")
	test("2011-13-13 00:00:00.000")
	test("2011-01-35 00:00:00.000")
	test("100-01-01 00:00:00.000")
	test("2010-02-04 21:00:67.012345678")
}

func TestFutureTimestamp(t *testing.T) {
	r := require.New(t)

	now := time.Now()

	// Test delay
	docTime := now.Add(-time.Hour * 24)
	r.True(documentDelayed(now.Sub(docTime), time.Hour*6, 0))
	r.False(documentDelayed(now.Sub(docTime), time.Hour*24, 0))

	// Test future delay
	docTime = now.Add(time.Hour * 24)
	r.False(documentDelayed(now.Sub(docTime), time.Hour*24, time.Hour*25))
	r.False(documentDelayed(now.Sub(docTime), time.Hour*25, time.Hour*25))
	r.False(documentDelayed(now.Sub(docTime), 0, time.Hour*24))
	r.False(documentDelayed(now.Sub(docTime), 0, time.Hour*25))
	r.True(documentDelayed(now.Sub(docTime), 0, time.Hour*24-1))
	r.True(documentDelayed(now.Sub(docTime), 0, time.Hour*23))

	r.False(documentDelayed(0, 0, 0))
}

func TestProcessBulkBulkIDsReset(t *testing.T) {
	mapping := seq.Mapping{
		"message": seq.NewSingleType(seq.TokenizerTypeText, "message", 1024),
	}
	tokenizers := map[seq.TokenizerType]tokenizer.Tokenizer{
		seq.TokenizerTypeText: tokenizer.NewTextTokenizer(1024, false, true, 8192),
	}

	proc := NewProcessor(mapping, tokenizers, 0, 0, false)

	readNext := func(docs ...string) func() ([]byte, error) {
		i := 0
		return func() ([]byte, error) {
			if i >= len(docs) {
				return nil, nil
			}
			d := []byte(docs[i])
			i++
			return d, nil
		}
	}

	_, _, _, err := proc.ProcessBulk(time.Now(), nil, nil, readNext(
		`{"message": "a"}`, `{"message": "b"}`, `{"message": "c"}`,
	))
	require.NoError(t, err)
	require.Len(t, proc.bulkIDs, 3)

	_, _, _, err = proc.ProcessBulk(time.Now(), nil, nil, readNext(
		`{"message": "a"}`, `{"message": "b"}`,
	))
	require.NoError(t, err)
	require.Len(t, proc.bulkIDs, 2)
}

func TestProcessorRidHash(t *testing.T) {
	mapping := seq.Mapping{
		"message": seq.NewSingleType(seq.TokenizerTypeText, "message", 1024),
	}
	tokenizers := map[seq.TokenizerType]tokenizer.Tokenizer{
		seq.TokenizerTypeText: tokenizer.NewTextTokenizer(1024, false, true, 8192),
	}

	proc := NewProcessor(mapping, tokenizers, 0, 0, true)

	doc := []byte(`{"message": "a", "ts": "2024-04-19 18:04:25.999"}`)

	_, metas, err := proc.ProcessDoc(doc, time.Now())
	require.NoError(t, err)
	require.NotEmpty(t, metas)

	id := metas[0].ID
	require.Equal(t, seq.RID(xxhash.Sum64(doc)), id.RID)

	doc2, metas2, err := proc.ProcessDoc(doc, time.Now())
	require.NoError(t, err)
	require.Nil(t, doc2)
	require.Nil(t, metas2)
}

func TestProcessBulkSkipsDuplicates(t *testing.T) {
	mapping := seq.Mapping{
		"message": seq.NewSingleType(seq.TokenizerTypeText, "message", 1024),
	}
	tokenizers := map[seq.TokenizerType]tokenizer.Tokenizer{
		seq.TokenizerTypeText: tokenizer.NewTextTokenizer(1024, false, true, 8192),
	}

	proc := NewProcessor(mapping, tokenizers, 0, 0, true)

	readNext := func(docs ...string) func() ([]byte, error) {
		i := 0
		return func() ([]byte, error) {
			if i >= len(docs) {
				return nil, nil
			}
			d := []byte(docs[i])
			i++
			return d, nil
		}
	}

	total, _, _, err := proc.ProcessBulk(time.Now(), nil, nil, readNext(
		`{"message": "a"}`, `{"message": "a"}`, `{"message": "b"}`,
	))
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Len(t, proc.bulkIDs, 2)
}

func TestParseRidMode(t *testing.T) {
	byHash, err := ParseRidMode("rand")
	require.NoError(t, err)
	require.False(t, byHash)

	byHash, err = ParseRidMode("hash")
	require.NoError(t, err)
	require.True(t, byHash)

	_, err = ParseRidMode("unknown")
	require.Error(t, err)
}

func TestRIDFromDocContent(t *testing.T) {
	r := require.New(t)

	mapping, err := seq.ReadMapping([]byte(`mapping-list:
  - type: keyword
    name: service
  - type: keyword
    name: kafka_request_correlation_id
`))
	r.NoError(err)

	newProc := func() *Processor {
		return NewProcessor(mapping, map[seq.TokenizerType]tokenizer.Tokenizer{
			seq.TokenizerTypeKeyword: tokenizer.NewKeywordTokenizer(20, false, true),
		}, 0, 0, true)
	}

	now := time.Now()
	doc := []byte(`{"ts": "2024-04-19 18:04:25.999", "service": "test", "kafka_request_correlation_id": "123"}`)

	// same content, same millisecond -> same ID (idempotent retries)
	_, metas1, err := newProc().ProcessDoc(doc, now)
	r.NoError(err)
	_, metas2, err := newProc().ProcessDoc(doc, now)
	r.NoError(err)
	r.Equal(metas1[0].ID, metas2[0].ID)
	r.Equal(seq.RID(xxhash.Sum64(doc)), metas1[0].ID.RID)

	// different content -> different RID (overwhelmingly likely)
	other := []byte(`{"ts": "2024-04-19 18:04:25.999", "service": "test", "kafka_request_correlation_id": "124"}`)
	_, metas3, err := newProc().ProcessDoc(other, now)
	r.NoError(err)
	r.NotEqual(metas1[0].ID.RID, metas3[0].ID.RID)

	// same content, different millisecond -> different MID
	otherTime := []byte(`{"ts": "2024-04-19 18:04:26.999", "service": "test", "kafka_request_correlation_id": "123"}`)
	_, metas4, err := newProc().ProcessDoc(otherTime, now)
	r.NoError(err)
	r.NotEqual(metas1[0].ID.MID, metas4[0].ID.MID)
	r.NotEqual(metas1[0].ID.RID, metas4[0].ID.RID) // timestamp is part of the content
}
