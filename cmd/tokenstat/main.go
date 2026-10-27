package main

import (
	"fmt"
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	app := kingpin.New("tokenstat", "analyzes token types of a sealed fraction and estimates typed repacking")

	classify := app.Command("classify", "show token type distribution, globally and per field")
	classifyFrac := classify.Arg("frac", "base file name of a fraction (without file suffix)").Required().String()
	classifyTop := classify.Flag("top", "number of fields to show").Default("40").Int()

	sample := app.Command("sample", "print random tokens of a field")
	sampleFrac := sample.Arg("frac", "base file name of a fraction (without file suffix)").Required().String()
	sampleField := sample.Flag("field", "field to sample from; empty means any field").String()
	sampleClass := sample.Flag("class", "only tokens of this class").Enum(classNames()...)
	sampleN := sample.Flag("n", "number of tokens").Short('n').Default("20").Int()
	sampleFloat := sample.Flag("float", "only tokens accepted by strconv.ParseFloat").Bool()

	lengths := app.Command("lengths", "show token length distribution of a field per class")
	lengthsFrac := lengths.Arg("frac", "base file name of a fraction (without file suffix)").Required().String()
	lengthsField := lengths.Flag("field", "field to analyze").Required().String()

	substr := app.Command("substr", "compare digit substring scan cost for string and typed layouts")
	substrFrac := substr.Arg("frac", "base file name of a fraction (without file suffix)").Required().String()
	substrField := substr.Flag("field", "field to search").Required().String()
	substrNeedles := substr.Flag("needle", "digit needle; repeatable").Required().Strings()
	substrBlockValues := substr.Flag("block-values", "values per typed block").Default("2048").Int()

	slowlog := app.Command("slowlog", "group digit substring needles of a slow query log by field and length")
	slowlogPath := slowlog.Arg("log", "slow query log (ndjson)").Required().ExistingFile()
	slowlogField := slowlog.Flag("field", "only needles of this field").String()
	slowlogFrac := slowlog.Flag("frac", "fraction to estimate typed scan share on").String()

	repack := app.Command("repack", "estimate .tokens size with typed token blocks")
	repackFrac := repack.Arg("frac", "base file name of a fraction (without file suffix)").Required().String()
	repackLevel := repack.Flag("level", "zstd level").Default("3").Int()
	repackBlockValues := repack.Flag("block-values", "values per typed block").Default("2048").Int()
	repackTop := repack.Flag("top", "number of fields to show").Default("30").Int()
	repackTypedZstd := repack.Flag("typed-zstd", "compress typed blocks with zstd on top of bitpack").Default("true").Bool()

	var err error
	switch kingpin.MustParse(app.Parse(os.Args[1:])) {
	case classify.FullCommand():
		err = runClassify(*classifyFrac, *classifyTop, os.Stdout)
	case sample.FullCommand():
		err = runSample(*sampleFrac, *sampleField, *sampleClass, *sampleFloat, *sampleN, os.Stdout)
	case lengths.FullCommand():
		err = runLengths(*lengthsFrac, *lengthsField, os.Stdout)
	case substr.FullCommand():
		err = runSubstr(*substrFrac, substrParams{
			field:       *substrField,
			needles:     *substrNeedles,
			blockValues: *substrBlockValues,
		}, os.Stdout)
	case slowlog.FullCommand():
		err = runSlowLog(*slowlogPath, *slowlogField, *slowlogFrac, os.Stdout)
	case repack.FullCommand():
		err = runRepack(*repackFrac, repackParams{
			level:       *repackLevel,
			blockValues: *repackBlockValues,
			typedZstd:   *repackTypedZstd,
			top:         *repackTop,
		}, os.Stdout)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(2)
	}
}
