package active

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
)

func BenchmarkDecision(b *testing.B) {
	sizes := []int{1000, 10000, 50000, 100000, 500000, 1000000}

	for _, size := range sizes {
		data := generateRandomUint32Slice(size)

		b.Run(fmt.Sprintf("RadixSort-%d", size), func(b *testing.B) {
			for b.Loop() {
				tmp := make([]uint32, size)
				copy(tmp, data)
				RadixSortUint32(tmp)
			}
		})

		b.Run(fmt.Sprintf("SliceSort-%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				tmp := make([]uint32, size)
				copy(tmp, data)
				slices.Sort(tmp)
			}
		})
	}
}

func generateRandomUint32Slice(size int) []uint32 {
	// Инициализируем генератор случайных чисел (если нужна настоящая случайность)
	// rand.Seed(time.Now().UnixNano()) // Для старых версий Go (< 1.20)

	slice := make([]uint32, size)
	for i := range slice {
		// Генерируем случайное uint32 число
		// Способ 1: используем rand.Uint32() (доступен с Go 1.8)
		slice[i] = rand.Uint32()

		// Альтернативный способ для обратной совместимости:
		// slice[i] = uint32(rand.Int63())
	}
	return slice
}

func RadixSortUint32(arr []uint32) {
	if len(arr) == 0 {
		return
	}

	n := len(arr)
	output := make([]uint32, n) // Вспомогательный массив для каждого прохода

	// Проходим по всем 4 байтам (0 - младший, 3 - старший)
	for byteIndex := 0; byteIndex < 4; byteIndex++ {
		// 1. Массив подсчета для 256 возможных значений байта
		count := [256]int{}

		// 2. Подсчет количества каждого значения байта
		for i := 0; i < n; i++ {
			// Извлекаем текущий байт (byteIndex) из числа
			byteValue := (arr[i] >> (byteIndex * 8)) & 0xFF
			count[byteValue]++
		}

		// 3. Преобразование в накопительные индексы (cumulative sum)
		// После этого count[i] содержит количество элементов с байтом <= i
		for i := 1; i < 256; i++ {
			count[i] += count[i-1]
		}

		// 4. Построение отсортированного по текущему байту массива
		// Идем с конца для сохранения стабильности (LSD)
		for i := n - 1; i >= 0; i-- {
			byteValue := (arr[i] >> (byteIndex * 8)) & 0xFF
			// Индекс для вставки: count[byteValue] - 1
			pos := count[byteValue] - 1
			output[pos] = arr[i]
			count[byteValue]--
		}

		// 5. Копируем результат обратно в исходный массив
		copy(arr, output)
	}
}
