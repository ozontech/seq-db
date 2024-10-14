package bytespool

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriter(t *testing.T) {
	t.Parallel()

	type Expect struct {
		OutputBeforeFlush string
		BufBeforeFlush    string
		OutputAfterFlush  string
		BufAfterFlush     string
		N                 int
	}
	type TestCase struct {
		Name       string
		WriterSize int
		Payload    []string
		Expect     []Expect
		Flush      []bool
	}

	tcs := []TestCase{
		{
			Name:       "invalid size and data",
			WriterSize: 0,
			Payload:    []string{"", "", ""},
			Expect:     []Expect{{}, {}, {}},
			Flush:      []bool{false, true, false},
		},
		{
			Name:       "empty data",
			WriterSize: 4,
			Payload:    []string{"", "", ""},
			Expect:     []Expect{{}, {}, {}},
			Flush:      []bool{true, false, true},
		},
		{
			Name:       "tiny buffer",
			WriterSize: 4,
			Payload:    []string{"more than four"},
			Expect: []Expect{
				{
					OutputBeforeFlush: "more than four",
					BufBeforeFlush:    "",
					OutputAfterFlush:  "more than four",
					BufAfterFlush:     "",
					N:                 len("more than four"),
				},
			},
			Flush: []bool{true},
		},
		{
			Name:       "large buffer",
			WriterSize: 1 << 10,
			Payload:    []string{"more than four"},
			Expect: []Expect{
				{
					OutputBeforeFlush: "",
					BufBeforeFlush:    "more than four",
					OutputAfterFlush:  "more than four",
					BufAfterFlush:     "",
					N:                 len("more than four"),
				},
			},
			Flush: []bool{true},
		},
		{
			Name:       "flush buffer state 1",
			WriterSize: 4,
			Payload:    []string{"123", "_this message must flush previous state"},
			Expect: []Expect{
				{
					OutputBeforeFlush: "",
					BufBeforeFlush:    "123",
					N:                 len("123"),
				},
				{
					OutputBeforeFlush: "123_this message must flush previous state",
					BufBeforeFlush:    "",
					OutputAfterFlush:  "123_this message must flush previous state",
					BufAfterFlush:     "",
					// '_' had to flushed
					N: len("this message must flush previous state"),
				},
			},
			Flush: []bool{false, true},
		},
		{
			Name:       "flush buffer state 2",
			WriterSize: 4,
			Payload:    []string{"1", "23", "456"},
			Expect: []Expect{
				{
					OutputBeforeFlush: "",
					BufBeforeFlush:    "1",
					N:                 1,
				},
				{
					OutputBeforeFlush: "",
					BufBeforeFlush:    "123",
					N:                 2,
				},
				{
					OutputBeforeFlush: "1234",
					BufBeforeFlush:    "56",
					OutputAfterFlush:  "123456",
					BufAfterFlush:     "",
					N:                 3,
				},
			},
			Flush: []bool{false, false, true},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			r := require.New(t)

			output := bytes.NewBuffer(nil)
			w := AcquireWriterSize(output, tc.WriterSize)
			defer FlushReleaseWriter(w)

			if len(tc.Payload) != len(tc.Expect) && len(tc.Payload) != len(tc.Flush) {
				panic(fmt.Errorf("BUG: invalid test case"))
			}

			for i, payload := range tc.Payload {
				n, err := w.WriteString(payload)
				r.NoError(err)
				r.Equal(tc.Expect[i].N, n)
				r.Equal(tc.Expect[i].OutputBeforeFlush, output.String())
				r.Equal(tc.Expect[i].BufBeforeFlush, string(w.Buf.B))
				if tc.Flush[i] {
					r.NoError(w.Flush())
					r.Equal(tc.Expect[i].OutputAfterFlush, output.String())
					r.Equal(tc.Expect[i].BufAfterFlush, string(w.Buf.B))
				}
			}
		})
	}
}
