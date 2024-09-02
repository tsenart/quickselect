package quickselect

import (
	"sort"
	"testing"
)

func TestQuickSelectWithSimpleArray(t *testing.T) {
	data := []int{50, 20, 30, 25, 45, 2, 6, 10, 3, 4, 5}
	lo, hi, err := QuickSelect(sort.IntSlice(data), 5)
	if err != nil {
		t.Errorf("Shouldn't have raised error: '%s'", err.Error())
	}
	smallestK := data[lo:hi]
	expectedK := []int{2, 3, 4, 5, 6}
	if !hasSameElements(smallestK, expectedK) {
		t.Errorf("Expected smallest K elements to be '%v', but got '%v'", expectedK, smallestK)
	}
}

func TestQuickSelectWithRepeatedElements(t *testing.T) {
	data := []int{2, 10, 5, 3, 2, 6, 2, 6, 10, 3, 4, 5}
	lo, hi, err := QuickSelect(sort.IntSlice(data), 5)
	if err != nil {
		t.Errorf("Shouldn't have raised error: '%s'", err.Error())
	}
	smallestK := data[lo:hi]
	expectedK := []int{2, 2, 2, 3, 3}
	if !hasSameElements(smallestK, expectedK) {
		t.Errorf("Expected smallest K elements to be '%v', but got '%v'", expectedK, smallestK)
	}
}

func TestQuickSelectEmptyDataStructure(t *testing.T) {
	data := sort.IntSlice([]int{})
	_, _, err := QuickSelect(data, 0)
	if err == nil {
		t.Errorf("Should have raised error on index outside of array length.")
	}

	_, _, err = QuickSelect(data, 5)
	if err == nil {
		t.Errorf("Should have raised error on index outside of array length.")
	}

	_, _, err = QuickSelect(data, -1)
	if err == nil {
		t.Errorf("Should have raised error on index outside of array length.")
	}
}

func TestNaiveSelectionFinding(t *testing.T) {
	fixtures := []struct {
		Array     []int
		ExpectedK []int
	}{
		{[]int{0, 14, 16, 29, 12, 2, 4, 4, 7, 29}, []int{0, 2, 4, 4}},
		{[]int{9, 3, 2, 18}, []int{9, 3, 2, 18}},
		{[]int{16, 29, -11, 25, 28, -14, 10, 4, 7, -27}, []int{-27, -11, -14, 4}},
		{[]int{10, 25, 15, 35, 26, 40, 55}, []int{10, 15, 25, 26}},
		{[]int{2, 10, 5, 3, 2, 6, 2, 6, 10, 3, 4, 5}, []int{2, 2, 2, 3}},
	}

	for _, fixture := range fixtures {
		lo, hi := naiveSelect(sort.IntSlice(fixture.Array), 4)
		resultK := fixture.Array[lo:hi]
		if !hasSameElements(resultK, fixture.ExpectedK) {
			t.Errorf("Expected smallest K elements to be '%v', but got '%v'", fixture.ExpectedK, resultK)
		}
	}
}

func TestHeapSelect(t *testing.T) {
	fixtures := []struct {
		Array     []int
		ExpectedK []int
	}{
		{[]int{0, 14, 16, 29, 12, 2, 4, 4, 7, 29}, []int{0, 2, 4, 4}},
		{[]int{9, 3, 2, 18}, []int{9, 3, 2, 18}},
		{[]int{16, 29, -11, 25, 28, -14, 10, 4, 7, -27}, []int{-27, -11, -14, 4}},
		{[]int{10, 25, 15, 35, 26, 40, 55}, []int{10, 15, 25, 26}},
		{[]int{2, 10, 5, 3, 2, 6, 2, 6, 10, 3, 4, 5}, []int{2, 2, 2, 3}},
		{[]int{2, 10, 5, 3, 2, 6, 2, 6, 10, 3, 4, 5}, []int{2, 2, 2, 3}},
		{[]int{10, 10, 6, 6, 5, 5, 4, 3, 3, 2, 2, 2}, []int{3, 2, 2, 2}},
	}

	for _, fixture := range fixtures {
		lo, hi := heapSelect(sort.IntSlice(fixture.Array), 4)
		resultK := fixture.Array[lo:hi]
		if !hasSameElements(resultK, fixture.ExpectedK) {
			t.Errorf("Expected smallest K elements to be '%v', but got '%v'", fixture.ExpectedK, resultK)
		}
	}
}

func hasSameElements(array1, array2 []int) bool {
	elements := make(map[int]int)

	for _, elem1 := range array1 {
		elements[elem1]++
	}

	for _, elem2 := range array2 {
		elements[elem2]--
	}

	for _, count := range elements {
		if count != 0 {
			return false
		}
	}
	return true
}

func hasSameElementsFloat64(array1, array2 []float64) bool {
	elements := make(map[float64]int)

	for _, elem1 := range array1 {
		elements[elem1]++
	}

	for _, elem2 := range array2 {
		elements[elem2]--
	}

	for _, count := range elements {
		if count != 0 {
			return false
		}
	}
	return true
}

func bench(b *testing.B, size, k int, quickselect bool) {
	b.StopTimer()
	data := make([]int, size)
	x := ^uint32(0)
	for i := 0; i < b.N; i++ {
		for n := size - 3; n <= size+3; n++ {
			for i := 0; i < len(data); i++ {
				x += x
				x ^= 1
				if int32(x) < 0 {
					x ^= 0x88888eef
				}
				data[i] = int(x % uint32(n/5))
			}
			if quickselect {
				b.StartTimer()
				QuickSelect(sort.IntSlice(data), k)
				b.StopTimer()
			} else {
				b.StartTimer()
				sort.Sort(sort.IntSlice(data))
				b.StopTimer()
			}
		}
	}
}

// Benchmarks for QuickSelect
func BenchmarkQuickSelectSize1e2K1e1(b *testing.B) { bench(b, 1e2, 1e1, true) }

func BenchmarkQuickSelectSize1e3K1e1(b *testing.B) { bench(b, 1e3, 1e1, true) }
func BenchmarkQuickSelectSize1e3K1e2(b *testing.B) { bench(b, 1e3, 1e2, true) }

func BenchmarkQuickSelectSize1e4K1e1(b *testing.B) { bench(b, 1e4, 1e1, true) }
func BenchmarkQuickSelectSize1e4K1e2(b *testing.B) { bench(b, 1e4, 1e2, true) }
func BenchmarkQuickSelectSize1e4K1e3(b *testing.B) { bench(b, 1e4, 1e3, true) }

func BenchmarkQuickSelectSize1e5K1e1(b *testing.B) { bench(b, 1e5, 1e1, true) }
func BenchmarkQuickSelectSize1e5K1e2(b *testing.B) { bench(b, 1e5, 1e2, true) }
func BenchmarkQuickSelectSize1e5K1e3(b *testing.B) { bench(b, 1e5, 1e3, true) }
func BenchmarkQuickSelectSize1e5K1e4(b *testing.B) { bench(b, 1e5, 1e4, true) }

func BenchmarkQuickSelectSize1e6K1e1(b *testing.B) { bench(b, 1e6, 1e1, true) }
func BenchmarkQuickSelectSize1e6K1e2(b *testing.B) { bench(b, 1e6, 1e2, true) }
func BenchmarkQuickSelectSize1e6K1e3(b *testing.B) { bench(b, 1e6, 1e3, true) }
func BenchmarkQuickSelectSize1e6K1e4(b *testing.B) { bench(b, 1e6, 1e4, true) }
func BenchmarkQuickSelectSize1e6K1e5(b *testing.B) { bench(b, 1e6, 1e5, true) }

func BenchmarkQuickSelectSize1e7K1e1(b *testing.B) { bench(b, 1e7, 1e1, true) }
func BenchmarkQuickSelectSize1e7K1e2(b *testing.B) { bench(b, 1e7, 1e2, true) }
func BenchmarkQuickSelectSize1e7K1e3(b *testing.B) { bench(b, 1e7, 1e3, true) }
func BenchmarkQuickSelectSize1e7K1e4(b *testing.B) { bench(b, 1e7, 1e4, true) }
func BenchmarkQuickSelectSize1e7K1e5(b *testing.B) { bench(b, 1e7, 1e5, true) }
func BenchmarkQuickSelectSize1e7K1e6(b *testing.B) { bench(b, 1e7, 1e6, true) }

func BenchmarkQuickSelectSize1e8K1e1(b *testing.B) { bench(b, 1e8, 1e1, true) }
func BenchmarkQuickSelectSize1e8K1e2(b *testing.B) { bench(b, 1e8, 1e2, true) }
func BenchmarkQuickSelectSize1e8K1e3(b *testing.B) { bench(b, 1e8, 1e3, true) }
func BenchmarkQuickSelectSize1e8K1e4(b *testing.B) { bench(b, 1e8, 1e4, true) }
func BenchmarkQuickSelectSize1e8K1e5(b *testing.B) { bench(b, 1e8, 1e5, true) }
func BenchmarkQuickSelectSize1e8K1e6(b *testing.B) { bench(b, 1e8, 1e6, true) }
func BenchmarkQuickSelectSize1e8K1e7(b *testing.B) { bench(b, 1e8, 1e7, true) }

// Benchmarks for sorting
func BenchmarkSortSize1e2K1e1(b *testing.B) { bench(b, 1e2, 1e1, false) }
func BenchmarkSortSize1e3K1e1(b *testing.B) { bench(b, 1e3, 1e1, false) }
func BenchmarkSortSize1e4K1e1(b *testing.B) { bench(b, 1e4, 1e1, false) }
func BenchmarkSortSize1e5K1e1(b *testing.B) { bench(b, 1e5, 1e1, false) }
func BenchmarkSortSize1e6K1e1(b *testing.B) { bench(b, 1e6, 1e1, false) }
func BenchmarkSortSize1e7K1e1(b *testing.B) { bench(b, 1e7, 1e1, false) }
func BenchmarkSortSize1e8K1e1(b *testing.B) { bench(b, 1e8, 1e1, false) }
