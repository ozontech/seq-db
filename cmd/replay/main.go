package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	modeProxy = "proxy"
	modeStore = "store"
)

func main() {
	app := kingpin.New("replay", "re-ingests documents from a fraction's .docs or .sdocs file into seq-db")

	replayCmd := &cmdReplay{}
	app.Arg("frac", "base file name of a fraction (without file suffix)").Required().StringVar(&replayCmd.frac)
	app.Flag("mode", "what to send documents to, proxy accepts ES bulk over HTTP, store accepts binary bulks over gRPC").
		Default(modeProxy).EnumVar(&replayCmd.mode, modeProxy, modeStore)
	app.Flag("address", "address of the proxy HTTP endpoint or the store gRPC endpoint").
		Default("http://localhost:9002").StringVar(&replayCmd.address)
	app.Flag("batch-size", "number of documents per bulk request").Default("1000").IntVar(&replayCmd.batchSize)
	app.Flag("mapping", "path to mapping YAML, required in store mode to build metas").ExistingFileVar(&replayCmd.mappingPath)
	app.Flag("dry-run", "only read the docs file and count documents without sending").BoolVar(&replayCmd.dryRun)

	kingpin.MustParse(app.Parse(os.Args[1:]))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := replayCmd.run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(2)
	}
}
