package s3

import (
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type client struct {
	*s3.Client
	bucket string
}
