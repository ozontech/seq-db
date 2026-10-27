package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

type proxySender struct {
	url     string
	client  *http.Client
	backoff func(attempt int) time.Duration
}

func newProxySender(address string) *proxySender {
	return &proxySender{
		url:     address + "/_bulk",
		client:  &http.Client{},
		backoff: backoffDelay,
	}
}

func (s *proxySender) Send(ctx context.Context, docs [][]byte) error {
	var body bytes.Buffer
	for _, doc := range docs {
		body.WriteString(`{"index":{}}` + "\n")
		body.Write(doc)
		body.WriteByte('\n')
	}

	var lastErr error
	for attempt := 0; attempt < maxBulkTries; attempt++ {
		retry, err := s.sendOnce(ctx, body.Bytes())
		if err == nil {
			return nil
		}

		if !retry {
			return err
		}

		lastErr = err
		logger.Warn(
			"bulk request failed, retrying",
			zap.Int("attempt", attempt+1),
			zap.Int("docs", len(docs)),
			zap.Error(err),
		)

		if err := sleep(ctx, s.backoff(attempt)); err != nil {
			return ctx.Err()
		}
	}

	return fmt.Errorf("giving up after %d attempts: %w", maxBulkTries, lastErr)
}

func (s *proxySender) sendOnce(ctx context.Context, body []byte) (retry bool, err error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		s.url,
		bytes.NewReader(body),
	)
	if err != nil {
		return false, fmt.Errorf("cannot create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return true, fmt.Errorf("bulk request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 400 {
		return false, nil
	}

	errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
	err = fmt.Errorf("bulk request failed with status %d: %s", resp.StatusCode, errBody)

	return resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500, err
}
