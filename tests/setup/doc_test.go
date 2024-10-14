package setup

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"lukechampine.com/frand"
)

func countSize(obj map[string]interface{}) int {
	size := len(obj)
	for _, v := range obj {
		if submap, ok := v.(map[string]interface{}); ok {
			size += countSize(submap)
		}
	}
	return size
}

func TestRandomJSON(t *testing.T) {
	for i := 0; i < 10000; i++ {
		size := frand.Intn(100) + 1
		str := RandomJSON(size)
		res := map[string]interface{}{}
		err := json.Unmarshal([]byte(str), &res)
		require.NoError(t, err, str)
		require.Equal(t, size, countSize(res))
	}
}

func TestRandomDocJSON(t *testing.T) {
	for i := 0; i < 10000; i++ {
		str := RandomDocJSON(frand.Intn(10)+1, frand.Intn(10))
		res := map[string]interface{}{}
		err := json.Unmarshal(str, &res)
		require.NoError(t, err, string(str))
		doc := &ExampleDoc{}
		err = json.Unmarshal(str, doc)
		require.NoError(t, err, string(str))
	}
}

func RunParallel(n int, f func()) {
	wait := sync.WaitGroup{}
	for j := 0; j < n; j++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			f()
		}()
	}
	wait.Wait()
}

func BenchmarkRandomJSON(b *testing.B) {
	RunParallel(4, func() {
		sum := 0
		for i := 0; i < b.N; i++ {
			res := RandomJSON(50)
			sum += len(res)
			_ = res
		}
		_ = sum
	})
}

func BenchmarkRandomDoc(b *testing.B) {
	RunParallel(4, func() {
		sum := 0
		for i := 0; i < b.N; i++ {
			res := RandomDoc(1)
			sum += len(res.Message)
			_ = res
		}
		_ = sum
	})
}

func BenchmarkGenerateDocs(b *testing.B) {
	res := GenerateDocs(b.N, func(_ int, doc *ExampleDoc) {
		*doc = *RandomDoc(1)
	})
	_ = res
}

func BenchmarkGenerateDocsJSON(b *testing.B) {
	res := GenerateDocsJSON(b.N, false)
	_ = res
}

func BenchmarkGenerateDocsJSONFields(b *testing.B) {
	res := GenerateDocsJSON(b.N, true)
	_ = res
}
