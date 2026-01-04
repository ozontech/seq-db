package setup

import (
	"encoding/json"
	"fmt"
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

func BenchmarkRandomJSON(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		sum := 0
		for pb.Next() {
			res := RandomJSON(50)
			sum += len(res)
			_ = res
		}
		_ = sum
	})
}

func BenchmarkRandomDoc(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		sum := 0
		for pb.Next() {
			res := RandomDoc(1)
			sum += len(res.Message)
			_ = res
		}
		_ = sum
	})
}

func BenchmarkGenerateDocs(b *testing.B) {
	sizes := []int{1000, 10_000, 1_000_000}

	for _, s := range sizes {
		b.Run(fmt.Sprintf("size=%d", s), func(b *testing.B) {
			var res []ExampleDoc

			for b.Loop() {
				res = GenerateDocs(s, func(_ int, doc *ExampleDoc) {
					*doc = *RandomDoc(1)
				})
			}

			if len(res) == 0 {
				b.FailNow()
			}
		})
	}
}

func BenchmarkGenerateDocsJSON(b *testing.B) {
	sizes := []int{1000, 10_000, 1_000_000}

	for _, s := range sizes {
		b.Run(fmt.Sprintf("size=%d", s), func(b *testing.B) {
			var res [][]byte

			for b.Loop() {
				res = GenerateDocsJSON(s, false)
			}

			if len(res) == 0 {
				b.FailNow()
			}
		})
	}
}

func BenchmarkGenerateDocsJSONFields(b *testing.B) {
	sizes := []int{1000, 10_000, 1_000_000}

	for _, s := range sizes {
		b.Run(fmt.Sprintf("size=%d", s), func(b *testing.B) {
			var res [][]byte

			for b.Loop() {
				res = GenerateDocsJSON(s, true)
			}

			if len(res) == 0 {
				b.FailNow()
			}
		})
	}
}
