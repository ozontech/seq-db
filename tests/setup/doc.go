package setup

import (
	"encoding/json"
	"runtime"
	"strings"
	"sync"
	"time"

	"lukechampine.com/frand" // much faster with multiple goroutines

	"github.com/ozontech/seq-db/tests/common"
)

// InlineJSON is a string representing valid json
// similar as json.RawMessage, but it's a string
type InlineJSON string

func (j *InlineJSON) MarshalJSON() ([]byte, error) {
	return []byte(*j), nil
}

func (j *InlineJSON) UnmarshalJSON(v []byte) error {
	*j = InlineJSON(v)
	return nil
}

// ExampleDoc is useful for testing and benchmarking
// instead of hardcoding json docs in code,
// you can hardcode struct which will be turned in json
type ExampleDoc struct {
	Service        string     `json:"service,omitempty"`
	Message        string     `json:"message,omitempty"`
	TraceID        string     `json:"traceID,omitempty"`
	Source         string     `json:"source,omitempty"`
	Zone           string     `json:"zone,omitempty"`
	RequiestObject InlineJSON `json:"requestObject,omitempty"`
	Level          int        `json:"level,omitempty"`
	Timestamp      time.Time  `json:"timestamp,omitempty"`
}

func randomStringFromPool(pool []string) string {
	ind := frand.Intn(len(pool))
	if pool[ind] == "" {
		return RandomString(3, 10)
	}
	return pool[ind]
}

// RandomString calls common.RandomString
// it is here, so you could call `setup.RandomSomething`, without exceptions
func RandomString(minSize, maxSize int) string {
	return common.RandomString(minSize, maxSize)
}

// ChanceValue returns given string with probability prob%, or empty string instead
func ChanceValue(prob int, value string) string {
	if frand.Intn(100) <= prob {
		return value
	}
	return ""
}

// RandomChanceString returns random string with probability prob%, or empty string instead
func RandomChanceString(prob, minSize, maxSize int) string {
	return ChanceValue(prob, RandomString(minSize, maxSize))
}

var services = []string{
	"metatarrificator", "trinity-storage", "product-facade", "rtb-api",
	"marketing-action-api", "s3front", "k8s-audit", "scout-collector",
	"", // will be random
}

func RandomService() string {
	return randomStringFromPool(services)
}

// lorem ipsum
var words = []string{
	"Suspendisse", "a", "massa", "eget", "turpis", "efficitur", "suscipit",
	"Proin", "quis", "nibh", "urna", "Suspendisse", "sed", "leo", "porta",
	"lacinia", "dui", "at", "viverra", "enim", "Cras", "sodales", "dolor",
	"eu", "malesuada", "suscipit", "Maecenas", "eget", "eros", "tempor", "felis",
	"varius", "dapibus", "a", "non", "ipsum", "Nulla", "facilisi", "Vivamus", "id",
	"", "", "", // will be random
}

func RandomWord() string {
	return randomStringFromPool(words)
}

var fields = []string{
	"service", "message", "trace_id", "source",
	"zone", "level", "timestamp", "requestObject",
	"", // will be random
}

func RandomField() string {
	return randomStringFromPool(fields)
}

func RandomSymbol() byte {
	symbols := "!@#$%^&*(){}[]:?/\\~;"
	return symbols[frand.Intn(len(symbols))]
}

func randomJSON(size int, builder *strings.Builder) {
	keys := map[string]struct{}{}
	// much faster to do with builders
	builder.WriteByte('{')
	for i := 0; i < size; i++ {
		// remove duplicate keys, because that's invalid json
		key := RandomString(1, 10)
		if _, ok := keys[key]; ok {
			i--
			continue
		}
		keys[key] = struct{}{}

		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteByte('"')
		builder.WriteString(key)
		// with some chance write new subsized json
		// else write just a string
		if frand.Intn(10) == 0 {
			subSize := min(frand.Intn(max(size/10, 1))+1, size-i-1)
			builder.WriteString(`":`)
			randomJSON(subSize, builder)
			i += subSize
		} else {
			builder.WriteString(`":"`)
			builder.WriteString(RandomString(3, 20))
			builder.WriteByte('"')
		}
	}
	builder.WriteByte('}')
}

// RandomText generates string with random words and symbols inbetween
func RandomText(size int) string {
	builder := strings.Builder{}
	for i := 0; i < size; i++ {
		builder.WriteString(RandomWord())
		if frand.Intn(10) == 0 {
			builder.WriteByte(RandomSymbol())
		}
		builder.WriteByte(' ')
	}
	return builder.String()
}

// RandomJSON generates json with random string keys
// and another random json or random string as values
func RandomJSON(size int) string {
	builder := strings.Builder{}
	randomJSON(max(1, size), &builder)
	return builder.String()
}

func RandomDoc(sizeScale int) *ExampleDoc {
	return &ExampleDoc{
		Service:        RandomService(),
		Message:        RandomText(10 * sizeScale),
		TraceID:        RandomChanceString(90, 8, 10),
		Source:         RandomChanceString(10, 8, 10),
		Zone:           randomStringFromPool([]string{"z23", "z501", "z502", "z26"}),
		Level:          frand.Intn(6),
		Timestamp:      time.Now(),
		RequiestObject: InlineJSON(RandomJSON(1 * sizeScale)),
	}
}

func MergeJSONs(a, b []byte) []byte {
	c := append([]byte(nil), a[:len(a)-1]...)
	c = append(c, ',')
	c = append(c, b[1:]...)
	return c
}

// RandomDocJSON creates random doc and adds to it some additional fields
func RandomDocJSON(sizeScale, addFields int) []byte {
	doc := RandomDoc(sizeScale)
	docbuf, err := json.Marshal(doc)
	if err != nil {
		panic(err)
	}
	if addFields == 0 {
		return docbuf
	}
	mp := map[string]string{}
	for i := 0; i < addFields; i++ {
		mp[RandomString(3, 10)] = RandomString(10, 20)
	}
	mapbuf, err := json.Marshal(mp)
	if err != nil {
		panic(err)
	}
	b := MergeJSONs(docbuf, mapbuf)
	return b
}

func DocsToStrings(docs []ExampleDoc) []string {
	docStr := make([]string, 0, len(docs))
	for i := 0; i < len(docs); i++ {
		b, err := json.Marshal(docs[i])
		if err != nil {
			panic(err)
		}
		docStr = append(docStr, string(b))
	}
	return docStr
}

func DocsFromStrings(docStr []string) []ExampleDoc {
	docs := make([]ExampleDoc, len(docStr))
	for i, doc := range docStr {
		err := json.Unmarshal([]byte(doc), &docs[i])
		if err != nil {
			panic(err)
		}
	}
	return docs
}

func splitRange(size int, callback func(from int, to int)) {
	routines := max(1, min(size/10000, runtime.NumCPU())) // do parallel for large arrays

	wait := sync.WaitGroup{}
	step := size / routines
	for i := 0; i < routines; i++ {
		wait.Add(1)

		from := i * step
		to := min(from+step, size)

		go func() {
			defer wait.Done()
			callback(from, to)
		}()
	}
	wait.Wait()
}

// GenerateDocs creates slice of docs and calls generator for each doc to will it with data
// If timestamp after call is zero, then this function will fill it with deterministic timestamp
// so you could query each doc by range, if needed
func GenerateDocs(size int, generator func(int, *ExampleDoc)) []ExampleDoc {
	start := time.Now()
	docs := make([]ExampleDoc, size)
	splitRange(size, func(from int, to int) {
		for i := from; i < to; i++ {
			generator(i, &docs[i])
			if docs[i].Timestamp.IsZero() {
				docs[i].Timestamp = start.Add(time.Millisecond * time.Duration(i))
			}
		}
	})
	return docs
}

// GenerateDocsJSON generates random docs straight in json
// just pass the output to `setup.BulkBytes` to store it
// faster ~2x if additionalFields=false
func GenerateDocsJSON(size int, additionalFields bool) [][]byte {
	docs := make([][]byte, size)
	splitRange(size, func(from int, to int) {
		for i := from; i < to; i++ {
			if !additionalFields {
				val, err := json.Marshal(RandomDoc(1))
				if err != nil {
					panic(err)
				}
				docs[i] = val
			} else {
				docs[i] = RandomDocJSON(1, 3)
			}
		}
	})
	return docs
}
