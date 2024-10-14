package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

const queryTpl = `{"index":"index-main","with_total":false}
{"query":{"bool":{"must":[{"query_string":{"query":%s}},{"range":{"timestamp":{"from":"%s","to":"%s"}}}]}},"from":%d,"size":%d}
`

type loadTask struct {
	fileName string
	query    string
	from     time.Time
	to       time.Time
	wg       *sync.WaitGroup
}

func loadFileWrk(ch chan *loadTask) {
	for t := range ch {
		err := loadFile(t.fileName, t.query, t.from, t.to)
		if err != nil {
			logger.Error("error load file",
				zap.String("file", t.fileName),
				zap.Time("from", t.from),
				zap.Time("to", t.to),
				zap.Error(err),
			)
		}
		t.wg.Done()
	}
}

func loadFile(fileName, query string, from, to time.Time) error {
	tmpFileName := fileName + ".tmp"
	fd, err := os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o664)
	if err != nil {
		return err
	}

	logger.Info("start load into file", zap.String("name", fileName))

	gzWriter, err := gzip.NewWriterLevel(fd, gzip.BestCompression)
	if err != nil {
		return err
	}

	if err := fetchToFileBatched(query, from, to, gzWriter); err != nil {
		return err
	}
	if err := gzWriter.Close(); err != nil {
		return err
	}
	if err := fd.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpFileName, fileName); err != nil {
		return err
	}
	return nil
}

func fetchToFileBatched(query string, from, to time.Time, writer io.Writer) error {
	offset := 0
	client := &http.Client{}
	newLine := []byte("\n")

	for {
		req, err := makeRequestJSON(query, from, to, offset, batchSize)
		if err != nil {
			return err
		}

		t := time.Now()
		host := getHost()
		resp, err := doSearchRequest(client, host, req)
		if err != nil {
			return err
		}

		logger.Info("got response",
			zap.String("host", host),
			zap.Duration("in", time.Since(t)),
		)

		docs, cnt, err := parse(resp)
		if err != nil {
			return err
		}

		if _, err := writer.Write(docs); err != nil {
			return err
		}

		if cnt < batchSize {
			break
		}

		if _, err := writer.Write(newLine); err != nil {
			return err
		}

		offset += cnt
	}

	return nil
}

func makeRequestJSON(query string, from, to time.Time, offset, limit int) (string, error) {
	queryStr, err := json.Marshal(query)
	if err != nil {
		return "", err
	}
	res := fmt.Sprintf(queryTpl, queryStr, from.UTC().Format(rangeFormat), to.UTC().Format(rangeFormat), offset, limit)
	return res, nil
}

func doSearchRequest(client *http.Client, host, q string) ([]byte, error) {
	res, err := client.Post("http://"+host+"/_msearch", "application/json", strings.NewReader(q))
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != 200 {
		return nil, fmt.Errorf("wrong status code: %d", res.StatusCode)
	}

	return body, nil
}
