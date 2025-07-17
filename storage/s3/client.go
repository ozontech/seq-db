package s3

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// client is a wrapper around [s3.Client] that holds bucket name.
type client struct {
	cli    *s3.Client
	bucket string
}

// NewClient returns new instance of [client].
//
// NOTE(dkharms): We might want to tweak smithy transport for
//   - IdleConnTimeout;
//   - MaxIdleConnsPerHost;
func NewClient(endpoint, accessKey, secretKey, region, bucket string) (*client, error) {
	credp := credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")

	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(region),
		config.WithBaseEndpoint(endpoint),
		config.WithCredentialsProvider(credp),
	)

	if err != nil {
		return nil, fmt.Errorf("cannot load S3 config: %w", err)
	}

	s3cli := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	return &client{s3cli, bucket}, nil
}
