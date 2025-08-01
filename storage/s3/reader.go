package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/ozontech/seq-db/storage"
)

var (
	_ storage.ImmutableFile = (*reader)(nil)
)

// reader is a wrapper around S3 client that provides basic IO functions.
// Be aware that [reader] is not thread-safe.
type reader struct {
	c        *Client
	filename string
	ctx      context.Context

	offset int64
	// size will be set after first [reader.getSize] call.
	size *int64
}

func NewReader(ctx context.Context, c *Client, filename string) *reader {
	return &reader{c: c, filename: filename, ctx: ctx}
}

func (r *reader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	out, err := r.c.cli.GetObject(r.ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.c.bucket),
		Key:    aws.String(r.filename),
		Range:  aws.String(r.rangeBytes(r.offset, int64(len(p)))),
	})
	if err != nil {
		return 0, fmt.Errorf(
			"s3: cannot read file=%q: %w",
			r.filename, err,
		)
	}
	defer out.Body.Close()

	expected := len(p)
	b, err := io.ReadFull(out.Body, p)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, fmt.Errorf(
			"s3: cannot copy body of file=%q: %w",
			r.filename, err,
		)
	}

	if b != expected {
		return 0, fmt.Errorf(
			"s3: short copy occurred: written=%d but expected=%d",
			b, expected,
		)
	}

	r.offset += int64(b)
	return expected, nil
}

func (r *reader) Seek(offset int64, whence int) (int64, error) {
	size, err := r.getSize()
	if err != nil {
		return 0, err
	}

	switch whence {
	case io.SeekStart:
		if offset < 0 || offset > size {
			return 0, errors.New("s3: offset will point outside of the file")
		}

		r.offset = offset
		return 0, nil
	case io.SeekCurrent:
		if r.offset+offset < 0 || r.offset+offset > size {
			return 0, errors.New("s3: offset will point outside of the file")
		}

		r.offset += offset
		return r.offset, nil
	case io.SeekEnd:
		if offset > 0 || size+offset < 0 {
			return 0, errors.New("s3: offset will point outside of the file")
		}

		r.offset = size + offset
		return r.offset, nil
	default:
		return 0, errors.New("s3: invalid seek anchor")
	}
}

func (r *reader) ReadAt(p []byte, off int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	out, err := r.c.cli.GetObject(r.ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.c.bucket),
		Key:    aws.String(r.filename),
		Range:  aws.String(r.rangeBytes(off, int64(len(p)))),
	})
	if err != nil {
		return 0, fmt.Errorf(
			"s3: cannot read file=%q at offset=%d with length=%d: %w",
			r.filename, off, len(p), err,
		)
	}

	defer out.Body.Close()

	expected := len(p)
	b, err := io.ReadFull(out.Body, p)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, fmt.Errorf(
			"s3: cannot copy body of file=%q: %w",
			r.filename, err,
		)
	}

	if b != expected {
		return 0, fmt.Errorf(
			"s3: short copy occurred: written=%d but expected=%d",
			b, expected,
		)
	}

	return expected, nil
}

// Close is noop for [reader] and underlying S3 client.
// S3 client handles resources automatically.
func (r *reader) Close() error {
	return nil
}

func (r *reader) Name() string {
	return r.filename
}

func (r *reader) Stat() (os.FileInfo, error) {
	out, err := r.c.cli.HeadObject(r.ctx, &s3.HeadObjectInput{
		Bucket: aws.String(r.c.bucket),
		Key:    aws.String(r.filename),
	})

	if err != nil {
		return nil, fmt.Errorf(
			"s3: cannot stat file=%q: %w",
			r.filename, err,
		)
	}

	return &fileStat{
		name:    r.filename,
		size:    *out.ContentLength,
		modTime: *out.LastModified,
	}, nil
}

// rangeBytes returns a valid content of a HTTP Range header.
// See more information here: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Range
func (r *reader) rangeBytes(start, length int64) string {
	return fmt.Sprintf("bytes=%d-%d", start, start+length-1)
}

func (r *reader) getSize() (int64, error) {
	if r.size != nil {
		return *r.size, nil
	}

	info, err := r.Stat()
	if err != nil {
		return 0, err
	}

	size := info.Size()
	r.size = &size

	return size, nil
}

var (
	_ os.FileInfo = (*fileStat)(nil)
)

type fileStat struct {
	name    string
	size    int64
	modTime time.Time
}

func (f *fileStat) IsDir() bool {
	return false
}

func (f *fileStat) ModTime() time.Time {
	return f.modTime
}

// Mode always returns -r--r--r--
// Such mode is perfectly fine since we store only immutable data in S3.
func (f *fileStat) Mode() fs.FileMode {
	return 0o444
}

func (f *fileStat) Name() string {
	return f.name
}

func (f *fileStat) Size() int64 {
	return f.size
}

func (f *fileStat) Sys() any {
	return nil
}
