package s3

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Client is a wrapper around [s3.Client] that holds bucket name.
type Client struct {
	cli    *s3.Client
	bucket string
}

// NewClient returns a new instance of a [Client].
//
// NOTE(dkharms): We might want to tweak smithy transport for
//   - IdleConnTimeout;
//   - MaxIdleConnsPerHost;
//
// And maybe we should add tracing support as well.
func NewClient(endpoint, accessKey, secretKey, region, bucket string) (*Client, error) {
	credp := credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")

	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(region),
		config.WithBaseEndpoint(endpoint),
		config.WithCredentialsProvider(credp),
		config.WithClientLogMode(aws.ClientLogMode(0)),
	)

	if err != nil {
		return nil, fmt.Errorf("cannot load S3 config: %w", err)
	}

	s3cli := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.DisableLogOutputChecksumValidationSkipped = true
	})

	return &Client{s3cli, bucket}, nil
}

func (c *Client) Exists(ctx context.Context, filename string) (bool, error) {
	_, err := c.cli.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(filename),
	})

	if err != nil {
		var s3err *s3types.NotFound
		if errors.As(err, &s3err) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (c *Client) Remove(ctx context.Context, filenames ...string) error {
	identifiers := make([]s3types.ObjectIdentifier, len(filenames))
	for i, filename := range filenames {
		identifiers[i] = s3types.ObjectIdentifier{Key: aws.String(filename)}
	}

	_, err := c.cli.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(c.bucket),
		Delete: &s3types.Delete{Objects: identifiers},
	})

	return err
}
