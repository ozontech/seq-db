package main

import (
	"fmt"
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

var decodableSections = decodableKinds

type cmdDecode struct {
	frac string
	only string
}

func (c *cmdDecode) run() error {
	return decodeFraction(c.frac, c.only, os.Stdout)
}

type cmdSeal struct {
	frac    string
	mapping string
}

func (c *cmdSeal) run() error {
	return sealFraction(
		c.frac,
		c.mapping,
		os.Stdin,
		defaultDocsPerDocBlock,
	)
}

// Launch as:
//
// > go run ./cmd/fraction/... decode <frac>
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
// > go run ./cmd/fraction/... decode --only=info,docs <frac>
//
// The `seal` command works the other way around: it reads JSON documents
// (one per line) from stdin and seals them into fraction files at <frac>,
// e.g. to produce fractions for decoding in tests:
//
// > cat docs.jsonl | go run ./cmd/fraction/... seal <frac>
// > cat docs.jsonl | go run ./cmd/fraction/... seal --mapping=mapping.yaml <frac>
func main() {
	app := kingpin.New("fraction", "seals and decodes seq-db fractions")

	decodeCmd := &cmdDecode{}
	decode := app.Command("decode", "decode a fraction into newline-delimited JSON")
	decode.Arg("frac", "base file name of a fraction (without file suffix)").Required().StringVar(&decodeCmd.frac)
	decode.Flag("only", "comma-separated list of sections to decode").
		HintOptions(decodableSections...).
		StringVar(&decodeCmd.only)

	sealCmd := &cmdSeal{}
	seal := app.Command("seal", "seal JSON documents (one per line) from stdin")
	seal.Arg("frac", "base file name of a fraction (without file suffix)").Required().StringVar(&sealCmd.frac)
	seal.Flag("mapping", "path to mapping YAML to use instead of the built-in default").
		ExistingFileVar(&sealCmd.mapping)

	switch kingpin.MustParse(app.Parse(os.Args[1:])) {
	case decode.FullCommand():
		if err := decodeCmd.run(); err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			os.Exit(2)
		}
	case seal.FullCommand():
		if err := sealCmd.run(); err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			os.Exit(2)
		}
	}
}
