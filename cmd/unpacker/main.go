package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/logger"
)

// Unpacks .docs file
func main() {
	if len(os.Args) < 2 {
		fmt.Println("No args")
		return
	}

	unpackFileName := os.Args[1]

	inFile, err := os.OpenFile(unpackFileName, os.O_RDONLY, 0o777)
	defer func() { _ = inFile.Close() }()

	if err != nil {
		logger.Fatal("error opening file", zap.Error(err))
	}

	outFileName := unpackFileName + ".unpacked"
	outFile, err := os.OpenFile(outFileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o777)
	if err != nil {
		logger.Fatal("error creating unpacked file", zap.Error(err))
	}
	defer func() {
		err := outFile.Close()
		if err != nil {
			logger.Error("error closing unpacked file", zap.String("name", outFileName))
		}
	}()

	stat, err := inFile.Stat()
	if err != nil {
		logger.Fatal("error getting file info", zap.Error(err))
	}
	total := stat.Size()

	reader := disk.NewDocsReader(disk.NewReader(1, nil), inFile, nil)

	offset := int64(0)

	logger.Info("unpacking", zap.String("filename", unpackFileName))
	docsBatch := make([]byte, 0)
	for {
		result, n, err := reader.ReadDocBlockPayload(offset)
		if err == io.EOF {
			logger.Info("unpack completed")
			return
		}
		if err != nil {
			logger.Fatal("error reading doc block", zap.Error(err))
		}
		offset += int64(n)
		docsBatch = docsBatch[:0]
		for len(result) != 0 {
			docsLen := binary.LittleEndian.Uint32(result[:4])
			docsBatch = append(docsBatch, result[4:docsLen+4]...)
			docsBatch = append(docsBatch, '\n')
			result = result[docsLen+4:]
		}
		_, err = outFile.Write(docsBatch)
		if err != nil {
			logger.Fatal("error writing to unpacked file", zap.Error(err))
		}

		logger.Info("unpacked",
			zap.Int64("offset", offset),
			zap.Int64("total", total),
		)
	}
}
