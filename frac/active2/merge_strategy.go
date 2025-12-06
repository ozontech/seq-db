package active2

import (
	"math"
)

/*
ПРИНЦИП ВЫБОРА КАНДИДАТОВ ДЛЯ СЛИЯНИЯ

1. ИСХОДНЫЕ ДАННЫЕ:
   items (индексы) → сгруппированы по ТИРАМ (tiers)

   Пример: 10 индексов распределены по 7 тирам

   │ Tier 0 │ Tier 1 │ Tier 2 │ Tier 3 │ Tier 4 │ Tier 5 │ Tier 6 │
   ├────────┼────────┼────────┼────────┼────────┼────────┼────────┤
   │   1    │   2    │   0    │   3    │   1    │   2    │   1    │
   └────────┴────────┴────────┴────────┴────────┴────────┴────────┘

2. ПОСТРОЕНИЕ РАСПРЕДЕЛЕНИЯ (buildTiersDistribution):
   Считаем количество индексов в каждом тире

3. ПОИСК ОКНА (mostPopulatedTiersRange):
   Скользящее окно размером winSize (по умолчанию 2 тира)

   winSize = round(bucketSizePercent / tierSizeDeltaPercent)
   Пример: 50% / 25% = 2 тира

   ┌─────────────────────────────────────────────────────┐
   │         Скользящее окно (размер = 2 тира)           │
   ├─────────────────────────────────────────────────────┤
   │ Окно 1: │ Tier 0 + Tier 1 │ = 1 + 2 = 3 элементов   │
   │ Окно 2: │ Tier 1 + Tier 2 │ = 2 + 0 = 2 элементов   │
   │ Окно 3: │ Tier 2 + Tier 3 │ = 0 + 3 = 3 элементов   │
   │ Окно 4: │ Tier 3 + Tier 4 │ = 3 + 1 = 4 элементов   | ← max!
   │ Окно 5: │ Tier 4 + Tier 5 │ = 1 + 2 = 3 элементов   │
   │ Окно 6: │ Tier 5 + Tier 6 │ = 2 + 1 = 3 элементов   │
   └─────────────────────────────────────────────────────┘

   Найденное окно: Tier 3-4 с 4 элементами
   Если элементов ≥ minToMerge → успех!

4. ПРАВИЛА ВЫБОРА:
   ┌─────────────────────────────────────────────────────┐
   │ Условие 1: элементов в окне ≥ minToMerge?           │
   │           Да → берём это окно                       │
   │           Нет → переходим к условию 2               │
   ├─────────────────────────────────────────────────────┤
   │ Условие 2: findAtAnyCost = true?                    │
   │           (len(items) >= forceMergeThreshold)       │
   │           Да → увеличиваем winSize в 2 раза         │
   │                 и ищем снова                        │
   │           Нет → возвращаем пустой результат         │
   └─────────────────────────────────────────────────────┘

5. ВЫДЕЛЕНИЕ КАНДИДАТОВ (extractIndexesInRange):
   Берём все индексы из найденного диапазона тиров

   Пример для окна Tier 3-4:
   ┌─────────────────────────────────────────┐
   │ До:     [1, 2, 0, 3, 1, 2, 1]           │
   │ Выбор:          ██ ██                    │
   │ Результат: 3 элемента из Tier 3         │
   │           + 1 элемент из Tier 4         │
   │           = 4 элемента всего            │
   └─────────────────────────────────────────┘

6. ПОВТОРЕНИЕ ПРОЦЕССА:
   Удаляем выбранные элементы из распределения
   Повторяем поиск, пока не останется окон
   с достаточным количеством элементов

   ┌─────────────────────────────────────────┐
   │ 1-я итерация: выбрали Tier 3-4 (4 elem) │
   │ 2-я итерация:                           │
   │ Распределение: [1, 2, 0, 0, 0, 2, 1]    │
   │ Находим новое окно...                   │
   └─────────────────────────────────────────┘

*/

// selectForMerge selects merge candidates based on their size.
// It groups items into sets within which the sizes of the items do not differ
// by more than a specified limit in percent (e.g. 50%)
func selectForMerge(items []memIndexExt, minToMerge int) [][]memIndexExt {
	if len(items) < minToMerge {
		return nil
	}

	tiersDist := buildTiersDistribution(items)
	findAtAnyCost := len(items) >= forceMergeThreshold
	winSize := int(math.Round(float64(bucketSizePercent) / tierSizeDeltaPercent))

	var res [][]memIndexExt
	for {
		countInRange, firstTier, lastTier := mostPopulatedTiersRange(tiersDist, minToMerge, winSize, findAtAnyCost)
		if countInRange == 0 {
			break
		}
		buf := make([]memIndexExt, 0, countInRange)
		res = append(res, extractIndexesInRange(items, buf, firstTier, lastTier, tiersDist))
	}
	return res
}

func buildTiersDistribution(items []memIndexExt) []int {
	lastTier := 0
	tiersDist := make([]int, maxTierCount)
	for _, index := range items {
		tiersDist[index.tier]++
		if index.tier > lastTier {
			lastTier = index.tier
		}
	}
	return tiersDist[:lastTier]
}

func extractIndexesInRange(items, buf []memIndexExt, firstTier, lastTier int, tiersDist []int) []memIndexExt {
	for _, index := range items {
		if firstTier <= index.tier && index.tier <= lastTier {
			buf = append(buf, index)
			tiersDist[index.tier]--
		}
	}
	return buf
}

func mostPopulatedTiersRange(tiersDist []int, minToMerge, winSize int, findAtAnyCost bool) (int, int, int) {
	var lastWinTier, maxWinSum int
	for {
		lastWinTier, maxWinSum = findMaxSumWindow(tiersDist, winSize)
		if maxWinSum >= minToMerge { // got it!
			break
		}
		if findAtAnyCost { // expand window size and find again
			// todo добавить логирования!
			winSize *= 2
			continue
		}
		return 0, 0, 0
	}

	firstTier := max(0, lastWinTier-winSize)
	lastTier := lastWinTier

	return maxWinSum, firstTier, lastTier
}

// sliding window sum
type winSum struct {
	buf []int
	sum int
	pos int
}

func (w *winSum) Add(v int) {
	w.sum += v - w.buf[w.pos]
	w.buf[w.pos] = v
	w.pos++
	if w.pos == len(w.buf) {
		w.pos = 0
	}
}

func findMaxSumWindow(tiersDist []int, winSize int) (int, int) {
	maxWinSum := 0
	lastWinTier := 0
	win := winSum{buf: make([]int, winSize)}

	for tier, size := range tiersDist {
		win.Add(size)
		if win.sum >= maxWinSum {
			lastWinTier = tier
			maxWinSum = win.sum
		}
	}
	return lastWinTier, maxWinSum
}
