package s3

import (
	"context"
	"fmt"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/ozontech/seq-db/storage"
)

var (
	_ storage.Uploader = (*uploader)(nil)
)

type uploader struct {
	c       *Client
	manager *manager.Uploader
}

func NewUploader(c *Client) *uploader {
	return &uploader{c: c, manager: manager.NewUploader(c.cli)}
}

func (u *uploader) Upload(ctx context.Context, r storage.ImmutableFile) error {
	_, err := u.manager.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(u.c.bucket),
		Key:    aws.String(path.Base(r.Name())),
		Body:   r,
	})

	if err != nil {
		return fmt.Errorf(
			"s3: cannot upload file=%q: %w",
			r.Name(), err,
		)
	}

	return nil
}
