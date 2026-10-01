package main

import (
	"bufio"
	"fmt"
	"io"
	"iter"
	"sync"
	"time"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tokenizer"
)

type docReader struct {
	scanner *bufio.Scanner

	proc *indexer.Processor

	docsPerDocBlock int
	bufEmpty        bool
}

func newDocReader(
	r io.Reader,
	mapping seq.Mapping,
	tokenizers map[seq.TokenizerType]tokenizer.Tokenizer,
	docsPerDockBlock int,
) (*docReader, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)

	return &docReader{
		scanner:         scanner,
		proc:            indexer.NewProcessor(mapping, tokenizers, 0, 0, 0),
		docsPerDocBlock: docsPerDockBlock,
	}, nil
}

func (d *docReader) readDocs(
	f *frac.Active,
) error {
	var wg sync.WaitGroup

	for readFn := range d.readIters() {
		_, binaryDocs, binaryMeta, err := d.proc.ProcessBulk(time.Now(), nil, nil, readFn)
		if err != nil {
			return fmt.Errorf("cannot process bulk: %w", err)
		}

		compressor := indexer.GetDocsMetasCompressor(docsCompressLevel, metaCompressLevel)

		compressor.CompressDocsAndMetas(binaryDocs, binaryMeta)
		docsBlock, metasBlock := compressor.DocsMetas()

		wg.Add(1)

		if err := f.Append(docsBlock, metasBlock, &wg); err != nil {
			return fmt.Errorf("cannot append docs to active fraction: %w", err)
		}

		wg.Wait()

		indexer.PutDocMetasCompressor(compressor)
	}

	return nil
}

type readNextFunc = func() ([]byte, error)

func (d *docReader) readIters() iter.Seq[readNextFunc] {
	return func(yield func(readNextFunc) bool) {
		for !d.bufEmpty {
			readLeft := d.docsPerDocBlock
			fn := func() ([]byte, error) {
				if readLeft == 0 {
					return nil, nil
				}

				b, err := d.readNext()
				if b == nil && err == nil {
					return nil, nil
				}

				readLeft--

				return b, err
			}

			if !yield(fn) {
				return
			}
		}
	}
}

func (d *docReader) readNext() ([]byte, error) {
	if !d.scanner.Scan() {
		d.bufEmpty = true

		if err := d.scanner.Err(); err != nil {
			return nil, err
		}

		return nil, nil
	}

	line := d.scanner.Bytes()
	if len(line) == 0 {
		return d.readNext()
	}

	return line, nil
}
