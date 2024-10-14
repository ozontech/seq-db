package main

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/ozontech/seq-db/logger"
)

const (
	batchSize     = 10000
	defaultWindow = 10 * time.Second
	rangeFormat   = "2006-01-02 15:04:05.999"
)

var getHost func() string

func getHostsGenerator(hosts []string) func() string {
	l := len(hosts)
	i := atomic.Uint64{}
	return func() string {
		i.Add(1)
		return hosts[int(i.Load())%l] + ":9002"
	}
}

func checkFileExists(file string) bool {
	_, err := os.Stat(file)
	if err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}

func loadAll(query string, from, to time.Time, filePattern string, window time.Duration, workers int) error {
	ch := make(chan *loadTask)
	for i := 0; i < workers; i++ {
		go loadFileWrk(ch)
	}

	wg := &sync.WaitGroup{}
	for subTo := to; !subTo.Before(from); subTo = subTo.Add(-window) {
		subFrom := subTo.Add(-window)

		fileName := fmt.Sprintf(filePattern, subFrom.Format("2006-01-02T15-04-05"))
		if checkFileExists(fileName) {
			logger.Info("file already loaded", zap.String("name", fileName))
			continue
		}

		wg.Add(1)
		ch <- &loadTask{
			fileName: fileName,
			query:    query,
			from:     subFrom,
			to:       subTo.Add(-time.Millisecond),
			wg:       wg,
		}
	}

	close(ch)
	wg.Wait()

	return nil
}

func main() {
	hostsStr := kingpin.Flag("hosts", `comma separated list of seq proxy hosts`).Required().String()
	dataDir := kingpin.Flag("out", `output directory with exported files`).Required().String()
	exportFrom := kingpin.Flag("from", `time in RFC3339 format: 2006-01-02T15:04:05Z07:00 (pay attention to time zone)`).Required().String()
	exportTo := kingpin.Flag("to", `time in RFC3339 format: 2006-01-02T15:04:05Z07:00 (pay attention to time zone)`).String()
	query := kingpin.Flag("query", `search query expression`).Required().String()
	windowSec := kingpin.Flag("window", `window for splitting a query into subqueries, sec`).Int()

	kingpin.Parse()

	hosts := strings.Split(*hostsStr, ",")
	getHost = getHostsGenerator(hosts)

	if *dataDir == "" {
		logger.Fatal("empty 'datadir' param")
	}
	err := os.Mkdir(*dataDir, 0o775)
	if err != nil && !os.IsExist(err) {
		logger.Fatal("mkdir error", zap.Error(err))
	}

	from, err := time.Parse(time.RFC3339, *exportFrom)
	if err != nil {
		logger.Fatal("error parsing 'from' param", zap.Error(err))
	}
	from = from.UTC()

	to := time.Now()
	if exportTo != nil && *exportTo != "" {
		to, err = time.Parse(time.RFC3339, *exportTo)
		if err != nil {
			logger.Fatal("error parsing 'to' param", zap.Error(err))
		}
		to = to.UTC()
	}

	window := defaultWindow
	if windowSec != nil && *windowSec != 0 {
		window = time.Duration(*windowSec) * time.Second
	}

	workers := len(hosts)

	fmt.Printf("Export query: %s\n", *query)
	fmt.Printf("Export interval (UTC): [%s - %s]\n", from.Format(time.RFC3339), to.Format(time.RFC3339))
	fmt.Printf("Export workers count: %d\n", workers)

	err = loadAll(*query, from, to, *dataDir+"/docs_%s.json.gz", window, workers)
	if err != nil {
		logger.Fatal("load error", zap.Error(err))
	}
}
