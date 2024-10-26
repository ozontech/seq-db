package bulk

import (
	"testing"
	"time"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/consts"
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
		for i := 0; i < b.N; i++ {
			_, err := time.Parse(consts.ESTimeFormat, toParse)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("handwritten", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, ok := parseESTime(toParse)
			if !ok {
				b.Fatal()
			}
		}
	})

	b.Run("rfc3339", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
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
