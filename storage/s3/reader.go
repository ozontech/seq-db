package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ozontech/seq-db/bytespool"
)

var (
	_ io.ReaderAt = (*reader)(nil)
)

var (
	// TODO(dkharms): adjust this parameter.
	pageSize = os.Getpagesize()
)

type reader struct {
	c        *client
	filename string
	ctx      context.Context
}

func NewReader(ctx context.Context, c *client, filename string) *reader {
	return &reader{c: c, filename: filename, ctx: ctx}
}

func (r *reader) ReadAt(p []byte, off int64) (n int, err error) {
	out, err := r.c.GetObject(r.ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.c.bucket),
		Key:    aws.String(r.filename),
		Range:  aws.String(r.rangeBytes(off, int64(len(p)))),
	})
	if err != nil {
		return 0, fmt.Errorf(
			"cannot read file=%q at offset=%d with length=%d: %w",
			r.filename, off, len(p), err,
		)
	}

	tmpBuf := bytespool.AcquireLen(pageSize)
	defer bytespool.Release(tmpBuf)
	defer out.Body.Close()

	dstBuf := bytes.NewBuffer(p)
	written, err := io.CopyBuffer(dstBuf, out.Body, tmpBuf.B)
	if err != nil {
		return 0, fmt.Errorf(
			"cannot copy body of file=%q: %w",
			r.filename, err,
		)
	}

	if written != int64(len(p)) {
		return 0, fmt.Errorf(
			"short copy occurred: written=%d but expected=%d",
			written, len(p),
		)
	}

	return len(p), nil

}

// rangeBytes returns valid content of Range header.
// See more information here: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Range
func (r *reader) rangeBytes(start, length int64) string {
	return fmt.Sprintf("bytes %d-%d/*", start, start+length-1)
}
