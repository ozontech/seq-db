package search

import (
	"io"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/seq"
)

func TestMergedDocsStream(t *testing.T) {
	n := 1000
	testDocs := make([]StreamingDoc, n)
	abErrs := [2][]error{}
	abDocs := [2][]StreamingDoc{}

	p := 0
	span := 0
	for i := 0; i < n; i++ {
		if span == 0 {
			span = rand.Intn(10) + 1
			p = (p + 1) % 2
		}
		testDocs[i] = StreamingDoc{
			ID:   seq.ID{MID: seq.MID(i), RID: seq.RID(i)},
			Data: []byte(strconv.Itoa(i)),
		}
		abDocs[p] = append(abDocs[p], testDocs[i])
		abErrs[p] = append(abErrs[p], nil)
		span--
	}

	stream := newMergedDocsStream(
		makeTestStream(abDocs[0], abErrs[0]),
		makeTestStream(abDocs[1], abErrs[1]),
		func(a, b seq.IDSource) bool { return seq.Less(a.ID, b.ID) },
	)

	actual := make([]StreamingDoc, 0)
	for doc, err := stream.Next(); err == nil; doc, err = stream.Next() {
		actual = append(actual, doc)
	}

	assert.Equal(t, testDocs, actual)

	stream = newMergedDocsStream(
		makeTestStream(abDocs[0], abErrs[0]),
		makeTestStream(abDocs[1], abErrs[1]),
		func(a, b seq.IDSource) bool { return seq.Less(a.ID, b.ID) },
	)

	actual = make([]StreamingDoc, 0)
	for doc, err := stream.Next(); err == nil; doc, err = stream.Next() {
		actual = append(actual, doc)
	}

	assert.Equal(t, testDocs, actual)
}

type testStream struct {
	err  error
	errs []error
	doc  StreamingDoc
	docs []StreamingDoc
}

func (t *testStream) Next() (StreamingDoc, error) {
	if len(t.docs) == 0 {
		return StreamingDoc{}, io.EOF
	}
	t.doc, t.docs = t.docs[0], t.docs[1:]
	if len(t.errs) > 0 {
		t.err, t.errs = t.errs[0], t.errs[1:]
	}
	return t.doc, t.err
}

func makeTestStream(docs []StreamingDoc, errs []error) DocsIterator {
	return &testStream{
		errs: errs,
		docs: docs,
	}
}

func TestCollapseBySources(t *testing.T) {
	sources := 100
	expectedDuplicates := 0
	origDocs := []StreamingDoc{}
	expectedDocs := []StreamingDoc{}
	for i := 0; i < 20; i++ {
		dataDocSet := false
		dataDoc := StreamingDoc{}
		withDataSource1 := rand.Intn(2 * sources)         // can be greater than sources, so it cause empty (not found) doc
		withDataSource2 := rand.Intn(2) + withDataSource1 // can be doubles
		for source := 0; source < sources; source++ {
			doc := StreamingDoc{
				ID:     seq.ID{MID: seq.MID(i)},
				Source: uint64(source),
			}
			if !dataDocSet {
				dataDoc = doc
				dataDocSet = true
			}
			if source == withDataSource1 || source == withDataSource2 {
				doc.Data = []byte(strconv.Itoa(i) + " - " + strconv.Itoa(source))
				dataDoc = doc
			}
			origDocs = append(origDocs, doc)
		}
		expectedDocs = append(expectedDocs, dataDoc)
		if withDataSource1 < sources &&
			withDataSource2 < sources &&
			withDataSource1 != withDataSource2 {
			expectedDuplicates++
		}
	}

	allDocs := []StreamingDoc{}
	origStream := makeTestStream(origDocs, nil)
	for doc, err := origStream.Next(); err == nil; doc, err = origStream.Next() {
		allDocs = append(allDocs, doc)
	}
	assert.Equal(t, origDocs, allDocs)

	origStream = makeTestStream(origDocs, nil)
	collapsedStream := newUniqueIDIterator(origStream)

	collapsedDocs := []StreamingDoc{}
	for doc, err := collapsedStream.Next(); err == nil; doc, err = collapsedStream.Next() {
		collapsedDocs = append(collapsedDocs, doc)
	}

	assert.Equal(t, expectedDuplicates, collapsedStream.duplicates)
	assert.Equal(t, expectedDocs, collapsedDocs)
}
