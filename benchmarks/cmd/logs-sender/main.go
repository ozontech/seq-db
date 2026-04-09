package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type config struct {
	datasetDir     string
	filePattern    string
	bulkURL        string
	indexName      string
	bulkSize       int
	queueCapacity  int
	requestTimeout time.Duration
}

type loader struct {
	cfg      config
	logsSent atomic.Int64
}

type bulk []string

func main() {
	cfg, err := loadConfig()
	if err != nil {
		fatalf("config error: %v", err)
	}

	if err := run(cfg); err != nil {
		fatalf("loader failed: %v", err)
	}
}

func run(cfg config) error {
	logFiles, err := listLogFiles(cfg.datasetDir, cfg.filePattern)
	if err != nil {
		return err
	}
	if len(logFiles) == 0 {
		return fmt.Errorf("no dataset files matched %q in %s", cfg.filePattern, cfg.datasetDir)
	}

	fmt.Printf("Starting loader\n")
	fmt.Printf("Dataset dir: %s\n", cfg.datasetDir)
	fmt.Printf("Matched files: %d\n", len(logFiles))
	fmt.Printf("Bulk URL: %s\n", cfg.bulkURL)
	fmt.Printf("Index name: %s\n", cfg.indexName)
	fmt.Printf("Bulk size: %d\n", cfg.bulkSize)
	fmt.Printf("Queue capacity: %d\n", cfg.queueCapacity)
	fmt.Printf("Senders per file: 2\n")

	l := &loader{cfg: cfg}
	httpClient := &http.Client{Timeout: cfg.requestTimeout}

	queues := make([]chan bulk, 0, len(logFiles))
	for range logFiles {
		queues = append(queues, make(chan bulk, cfg.queueCapacity))
	}

	var monitorWG sync.WaitGroup
	done := make(chan struct{})
	monitorWG.Add(1)
	go func() {
		defer monitorWG.Done()
		l.monitorProgress(done)
	}()

	var readerWG sync.WaitGroup
	for i, file := range logFiles {
		readerWG.Add(1)
		go func(filePath string, ch chan<- bulk) {
			defer readerWG.Done()
			if err := l.readFile(filePath, ch); err != nil {
				fatalf("reader error for %s: %v\n", filePath, err)
			}
		}(file, queues[i])
	}

	var senderWG sync.WaitGroup
	for i := range queues {
		for j := 0; j < 2; j++ {
			workerID := i*2 + j
			senderWG.Add(1)
			go func(id int, ch <-chan bulk) {
				defer senderWG.Done()
				l.sender(id, ch, httpClient)
			}(workerID, queues[i])
		}
	}

	readerWG.Wait()
	for _, q := range queues {
		close(q)
	}
	senderWG.Wait()
	close(done)
	monitorWG.Wait()

	fmt.Printf("\nCompleted ingestion\n")
	fmt.Printf("Total logs processed: %d\n", l.logsSent.Load())
	return nil
}

func (l *loader) monitorProgress(done <-chan struct{}) {
	interval := 5 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var sentPrev int64
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			sent := l.logsSent.Load()
			rate := float64(sent-sentPrev) / interval.Seconds()
			sentPrev = sent
			fmt.Printf("Progress: %d logs | Rate: %.2f logs/s\n", sent, rate)
		}
	}
}

func (l *loader) readFile(filePath string, out chan<- bulk) error {
	fmt.Printf("Starting reader for %s\n", filePath)
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)

	current := make(bulk, 0, l.cfg.bulkSize)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		current = append(current, line)
		if len(current) >= l.cfg.bulkSize {
			l.offerWithBackoff(out, current)
			current = make(bulk, 0, l.cfg.bulkSize)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if len(current) > 0 {
		l.offerWithBackoff(out, current)
	}
	fmt.Printf("Completed reader for %s\n", filePath)
	return nil
}

func (l *loader) offerWithBackoff(out chan<- bulk, b bulk) {
	for {
		select {
		case out <- b:
			return
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (l *loader) sender(workerID int, in <-chan bulk, client *http.Client) {
	fmt.Printf("Starting sender %d\n", workerID)
	for blk := range in {
		if err := l.sendBulk(client, blk); err != nil {
			fmt.Printf("sender %d failed to send bulk: %v\n", workerID, err)
			continue
		}
		l.logsSent.Add(int64(len(blk)))
	}
	fmt.Printf("Completed sender %d\n", workerID)
}

func (l *loader) sendBulk(client *http.Client, b bulk) error {
	body, err := buildBulkRequest(l.cfg.indexName, b)
	if err != nil {
		return err
	}
	return sendOnce(client, l.cfg.bulkURL, body)
}

func sendOnce(client *http.Client, bulkURL string, body []byte) error {
	req, err := http.NewRequest(http.MethodPost, bulkURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http error %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	var parsed struct {
		Errors bool `json:"errors"`
	}
	if err := json.Unmarshal(respBody, &parsed); err == nil && parsed.Errors {
		fmt.Printf("bulk response contains item-level errors\n")
	}

	return nil
}

func buildBulkRequest(indexName string, records []string) ([]byte, error) {
	if len(records) == 0 {
		return nil, errors.New("empty bulk")
	}
	action := fmt.Sprintf(`{"index":{"_index":%q}}`, indexName)

	var buf bytes.Buffer
	for _, record := range records {
		buf.WriteString(action)
		buf.WriteString("\n")
		buf.WriteString(record)
		buf.WriteString("\n")
	}
	return buf.Bytes(), nil
}

func listLogFiles(datasetDir, filePattern string) ([]string, error) {
	entries, err := os.ReadDir(datasetDir)
	if err != nil {
		return nil, err
	}

	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ok, err := filepath.Match(filePattern, entry.Name())
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		files = append(files, filepath.Join(datasetDir, entry.Name()))
	}
	sort.Strings(files)
	return files, nil
}

func loadConfig() (config, error) {
	cfg := config{
		datasetDir:     getEnv("DATASET_DIR", "/dataset"),
		filePattern:    getEnv("FILE_PATTERN", "docs-*.log"),
		bulkURL:        os.Getenv("BULK_URL"),
		indexName:      getEnv("INDEX_NAME", "logs-index"),
		bulkSize:       getEnvInt("BULK_SIZE", 4096),
		queueCapacity:  getEnvInt("QUEUE_CAPACITY", 10),
		requestTimeout: getEnvDuration("REQUEST_TIMEOUT", 5*time.Second),
	}

	if strings.TrimSpace(cfg.bulkURL) == "" {
		return cfg, errors.New("BULK_URL must be set")
	}
	if cfg.bulkSize <= 0 {
		return cfg, errors.New("BULK_SIZE must be positive")
	}
	if cfg.queueCapacity <= 0 {
		return cfg, errors.New("QUEUE_CAPACITY must be positive")
	}
	return cfg, nil
}

func getEnv(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func getEnvInt(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
