package util

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrorsCollapser(t *testing.T) {
	r := require.New(t)

	// 2 same errors
	err := CollapseErrors([]error{nil, io.EOF, nil, io.EOF})
	r.NotNil(err)
	r.Equal(err.Error(), "EOF (got 2 times)")
	r.True(errors.Is(err, io.EOF))

	// deduplicate 2 errors
	err = DeduplicateErrors([]error{nil, io.EOF, nil, io.EOF, context.Canceled})
	r.NotNil(err)
	str := err.Error()
	r.True(str == "EOF; context canceled" || str == "context canceled; EOF", str)
	r.True(errors.Is(err, io.EOF))
	r.True(errors.Is(err, context.Canceled))

	// 1 error
	err = CollapseErrors([]error{nil, io.EOF, nil, nil, nil})
	r.NotNil(err)
	// check did not write "(got N times)" if there is only 1 error
	r.Equal(err.Error(), "EOF")

	// nils
	r.Nil(CollapseErrors([]error{nil, nil, nil, nil}))
	r.Nil(CollapseErrors(nil))
	r.Nil(CollapseErrors([]error{}))

	// mixed errors
	err = CollapseErrors([]error{io.EOF, io.EOF, nil, context.Canceled, context.Canceled, context.DeadlineExceeded})
	r.NotNil(err)
	str = err.Error()
	r.Contains(str, "EOF (got 2 times)")
	r.Contains(str, "context canceled (got 2 times)")
	r.Contains(str, "context deadline exceeded")
	r.NotContains(str, "(got 1 times)")
	r.ErrorIs(err, io.EOF)
	r.ErrorIs(err, context.Canceled)
	r.ErrorIs(err, context.DeadlineExceeded)
}
