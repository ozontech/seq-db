package s3

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type readSeekerAt interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}

type uploader struct {
	c        *client
	filename string
	ctx      context.Context
}

func NewUploader(ctx context.Context, c *client, filename string) *uploader {
	return &uploader{c: c, filename: filename, ctx: ctx}
}

func (w *uploader) Upload(r readSeekerAt) error {
	uploader := manager.NewUploader(w.c.cli)

	_, err := uploader.Upload(w.ctx, &s3.PutObjectInput{
		Bucket: aws.String(w.c.bucket),
		Key:    aws.String(w.filename),
		Body:   r,
	})

	if err != nil {
		return fmt.Errorf(
			"cannot upload file=%q: %w",
			w.filename, err,
		)
	}

	return nil
}
