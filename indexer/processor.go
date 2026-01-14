package indexer

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	insaneJSON "github.com/ozontech/insane-json"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/consts"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tokenizer"
	"github.com/ozontech/seq-db/util"
)

var (
	parseErrors  = bulkTimeErrors.WithLabelValues("parse_error")
	delays       = bulkTimeErrors.WithLabelValues("delay")
	futureDelays = bulkTimeErrors.WithLabelValues("future_delay")
)

// Processor accumulates meta and docs from a single bulk
// returns bulk request ready to be sent to store
type Processor struct {
	proxyIndex  uint64
	drift       time.Duration
	futureDrift time.Duration

	indexer *indexer
	decoder *insaneJSON.Root
}

func init() {
	// Disable cache for the Dig() method.
	insaneJSON.MapUseThreshold = math.MaxInt32
}

func NewProcessor(mapping seq.Mapping, tokenizers map[seq.TokenizerType]tokenizer.Tokenizer, drift, futureDrift time.Duration, index uint64) *Processor {
	return &Processor{
		proxyIndex:  index,
		drift:       drift,
		futureDrift: futureDrift,
		indexer: &indexer{
			tokenizers: tokenizers,
			mapping:    mapping,
			metas:      []MetaData{},
		},
		decoder: insaneJSON.Spawn(),
	}
}

var errNotAnObject = errors.New("not an object")

func (p *Processor) ProcessDoc(doc []byte, requestTime time.Time) ([]byte, []MetaData, error) {
	err := p.decoder.DecodeBytes(doc)
	if err != nil {
		return nil, nil, err
	}
	if !p.decoder.IsObject() {
		return nil, nil, errNotAnObject
	}
	docTime, timeField := extractDocTime(p.decoder.Node, requestTime)
	docDelay := requestTime.Sub(docTime)
	if timeField == nil { // couldn't parse given event time
		parseErrors.Inc()
	} else if documentDelayed(docDelay, p.drift, p.futureDrift) {
		docTime = requestTime
	}

	id := seq.NewID(docTime, (rand.Uint64()<<16)+p.proxyIndex)

	p.indexer.Index(p.decoder.Node, id, uint32(len(doc)))

	return doc, p.indexer.Metas(), nil
}

func documentDelayed(docDelay, drift, futureDrift time.Duration) bool {
	delayed := false
	if docDelay > drift && drift > 0 {
		delays.Inc()
		delayed = true
	}
	if docDelay < 0 && docDelay.Abs() > futureDrift && futureDrift > 0 {
		futureDelays.Inc()
		delayed = true
	}
	return delayed
}

func extractDocTime(node *insaneJSON.Node, requestTime time.Time) (time.Time, []string) {
	for _, field := range consts.TimeFields {
		timeVal := node.Dig(field...).AsBytes()
		if len(timeVal) == 0 {
			continue
		}

		for _, f := range consts.TimeFormats {
			var t time.Time
			var ok bool
			if f == consts.ESTimeFormat {
				// Fallback to optimized es time parsing.
				t, ok = parseESTime(util.ByteToStringUnsafe(timeVal))
			} else {
				var err error
				t, err = time.Parse(f, util.ByteToStringUnsafe(timeVal))
				ok = err == nil
			}
			if ok {
				return t, field
			}
		}
	}
	defaultTime := requestTime
	return defaultTime, nil
}

// parseESTime parses time in "2006-01-02 15:04:05.999" format.
// It is copied and modified stdlib function time.parseRFC3339.
func parseESTime(t string) (time.Time, bool) {
	if len(t) < len("2006-01-02 15:04:05") {
		return time.Time{}, false
	}

	ok := true
	parseUint := func(s string, from, to uint) uint {
		x := uint(0)
		for _, c := range []byte(s) {
			if c < '0' || c > '9' {
				ok = false
				return 0
			}
			x = x*10 + uint(c) - '0'
		}
		if x < from || x > to {
			ok = false
			return 0
		}
		return x
	}

	year := parseUint(t[0:4], 0, 9999) // Parse YYYY
	month := parseUint(t[5:7], 1, 12)  // Parse MM
	// Day in a month will be checked in the Date function.
	day := parseUint(t[8:10], 1, 31)     // Parse DD
	hour := parseUint(t[11:13], 0, 23)   // Parse HH
	minute := parseUint(t[14:16], 0, 59) // Parse mm
	second := parseUint(t[17:19], 0, 59) // Parse ss
	if !ok || !(t[4] == '-' && t[7] == '-' && t[10] == ' ' && t[13] == ':' && t[16] == ':') {
		return time.Time{}, false
	}

	t = t[19:]
	nsecs := uint(0)
	if t != "" {
		if t[0] != '.' || len(t) == 1 {
			return time.Time{}, false
		}
		t = t[1:]

		// Parse nanoseconds.
		multi := uint(math.Pow10(9 - len(t)))
		if multi == 0 {
			multi = 1
		}
		nsecs = parseUint(t, 0, 999999999) * multi
		if !ok {
			return time.Time{}, false
		}
	}

	return time.Date(int(year), time.Month(month), int(day), int(hour), int(minute), int(second), int(nsecs), time.UTC), true
}

func (p *Processor) ProcessBulk(
	requestTime time.Time,
	dstDocs, dstMeta []byte,
	readNext func() ([]byte, error),
) (int, []byte, []byte, error) {
	parseDuration := time.Duration(0)

	total := 0
	for {
		originalDoc, err := readNext()
		if err != nil {
			return 0, nil, nil, fmt.Errorf("reading next document: %s", err)
		}
		if originalDoc == nil {
			break
		}
		parseStart := time.Now()
		doc, meta, err := p.ProcessDoc(originalDoc, requestTime)
		if err != nil {
			if errors.Is(err, errNotAnObject) {
				logger.Error("unable to process the document because it is not an object", zap.Any("document", json.RawMessage(originalDoc)))
				notAnObjectTotal.Inc()
				continue
			}
			return 0, nil, nil, fmt.Errorf("processing doc: %s", err)
		}
		parseDuration += time.Since(parseStart)

		total++
		dstDocs = binary.LittleEndian.AppendUint32(dstDocs, uint32(len(doc)))
		dstDocs = append(dstDocs, doc...)
		for _, m := range meta {
			// todo: it is possible to have a few equal tokens here
			// todo: probably we need deduplicate it here
			dstMeta = marshalAppendMeta(dstMeta, m)
		}
	}

	bulkParseDurationSeconds.Observe(parseDuration.Seconds())

	return total, dstDocs, dstMeta, nil
}

func marshalAppendMeta(dst []byte, meta MetaData) []byte {
	metaLenPosition := len(dst)
	dst = append(dst, make([]byte, 4)...)
	dst = meta.MarshalBinaryTo(dst)
	// Metadata length = len(slice after append) - len(slice before append).
	metaLen := uint32(len(dst) - metaLenPosition - 4)
	// Put metadata length before metadata bytes.
	binary.LittleEndian.PutUint32(dst[metaLenPosition:], metaLen)
	return dst
}
