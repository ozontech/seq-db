package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	address := flag.String("address", "http://localhost:9002", "Elasticsearch address")
	file := flag.String("file", "", "Path to JSONLines file")
	batchSize := flag.Int("batch-size", 1000, "Number of documents per batch")
	interval := flag.Duration("interval", 0, "Time between inserting batches")

	flag.Parse()

	if *file == "" {
		log.Fatal("file flag is required")
	}

	f, err := os.Open(*file)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	const maxCapacity = 1024 * 1024 // 1MB buffer
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity) // Set custom buffer and max size

	var batch []string
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		if line == "" {
			continue
		}

		// Validate JSON
		//var doc map[string]interface{}
		//if err := json.Unmarshal([]byte(line), &doc); err != nil {
		//    log.Printf("Warning: skipping invalid JSON at line %d: %v", lineNum, err)
		//    continue
		//}

		// Create index action
		action := fmt.Sprintf(`{"index":{}}`)

		batch = append(batch, action, line)

		if len(batch)/2 >= *batchSize {
			if err := sendBatch(*address, batch); err != nil {
				log.Printf("Failed to send batch: %v", err)
			} else {
				log.Printf("Sent %d documents", len(batch)/2)
			}
			batch = nil

			if *interval > 0 {
				time.Sleep(*interval)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error reading file: %v", err)
	}

	// Send remaining batch
	if len(batch) > 0 {
		if err := sendBatch(*address, batch); err != nil {
			log.Printf("Failed to send final batch: %v", err)
		} else {
			log.Printf("Sent final %d documents", len(batch)/2)
		}
	}
}

func sendBatch(address string, batch []string) error {
sendbatchagain:

	url := address

	body := bytes.NewBufferString("")
	for _, line := range batch {
		body.WriteString(line)
		body.WriteString("\n")
	}

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		if resp.StatusCode == 429 {
			log.Printf("429 received. Retrying...")
			time.Sleep(5 * time.Second)
			goto sendbatchagain
		}

		var buf bytes.Buffer
		buf.ReadFrom(resp.Body)
		return fmt.Errorf("bulk request failed with status %d: %s", resp.StatusCode, buf.String())
	}

	return nil
}
