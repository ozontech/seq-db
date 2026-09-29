package main

import (
	"fmt"
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

var decodableSections = decodableKinds

var (
	flagFrac = kingpin.Arg("frac", "base file name of a fraction (without file suffix)").Required().String()

	flagOnly = kingpin.Flag("only", "comma-separated list of sections to decode").
			HintOptions(decodableSections...).
			String()

	flagSeal = kingpin.Flag("seal", "read JSON documents (one per line) from stdin and seal them into <frac>").
			Bool()

	flagMapping = kingpin.Flag("mapping", "path to mapping YAML to use with --seal instead of the built-in default").
			ExistingFile()
)

// Launch as:
//
// > go run ./cmd/fraction-decoder/... <frac>
//
// where <frac> is the base file name of a sealed fraction (without suffix),
// e.g. ./data/store/frac_000123. Decoded content is written to stdout as
// newline-delimited JSON, one record per line, tagged with a "kind" field.
//
// Supports both the current split-file layout
// (.info/.docs/.tokens/.offsets/.ids/.lids) and the legacy single .index file.
//
// Use --only to restrict what gets decoded:
//
// > go run ./cmd/fraction-decoder/... --only=info,docs <frac>
//
// With --seal the tool works the other way around: it reads JSON documents
// (one per line) from stdin and seals them into fraction files at <frac>,
// e.g. to produce fractions for decoding in tests:
//
// > cat docs.jsonl | go run ./cmd/fraction-decoder/... --seal <frac>
// > cat docs.jsonl | go run ./cmd/fraction-decoder/... --seal --mapping=mapping.yaml <frac>
func main() {
	kingpin.Parse()

	if *flagSeal {
		if err := sealFraction(*flagFrac, *flagMapping, os.Stdin); err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())

			os.Exit(2)
		}

		return
	}

	only := ""
	if flagOnly != nil {
		only = *flagOnly
	}

	if err := decodeFraction(*flagFrac, only, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())

		os.Exit(2)
	}
}
