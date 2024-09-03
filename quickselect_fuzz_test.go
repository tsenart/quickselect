package quickselect

import (
	"encoding/binary"
	"math/rand/v2"
	"sort"
	"testing"
)

func Fuzz_QuickSelect(f *testing.F) {
	fuzzSelect(f, "QuickSelect", QuickSelect)
}

func Fuzz_quickSelect(f *testing.F) {
	fuzzSelect(f, "quickSelect", quickSelect)
}

func Fuzz_heapSelect(f *testing.F) {
	fuzzSelect(f, "heapSelect", heapSelect)
}

func Fuzz_naiveSelect(f *testing.F) {
	fuzzSelect(f, "naiveSelect", naiveSelect)
}

func fuzzSelect(f *testing.F, name string, selectFunc func(sort.Interface, int) (int, int)) {
	f.Helper()

	for _, n := range []int{0, 1, 10, 100} {
		for _, k := range []int{0, 1, 10, 100} {
			sorted := make([]byte, n*8)
			for i := 0; i < n; i++ {
				binary.BigEndian.PutUint64(sorted[i*8:(i+1)*8], uint64(i))
			}
			f.Add(sorted, uint(k))

			shuffled := make([]byte, len(sorted))
			copy(shuffled, sorted)

			rand.Shuffle(n, func(i, j int) {
				copy(shuffled[i*8:(i+1)*8], sorted[j*8:(j+1)*8])
				copy(shuffled[j*8:(j+1)*8], sorted[i*8:(i+1)*8])
			})
			f.Add(shuffled, uint(k))

			inverted := make([]byte, len(sorted))
			for i := 0; i < n; i++ {
				binary.BigEndian.PutUint64(inverted[(n-1-i)*8:(n-i)*8], uint64(i))
			}
			f.Add(inverted, uint(k))
		}
	}

	f.Fuzz(func(t *testing.T, data []byte, k uint) {
		if len(data)%8 != 0 {
			return // Skip if data length is not multiple of 8
		}

		n := len(data) / 8

		// Convert []byte to []int
		intData := make([]int, n)
		for i := 0; i < n; i++ {
			intData[i] = int(binary.BigEndian.Uint64(data[i*8 : (i+1)*8]))
		}

		if len(intData) == 0 {
			return // Skip empty slices
		}

		if k < 1 || k > uint(len(intData)) {
			return // Skip invalid k values
		}

		// Create a copy for sorting (to compare results)
		sortedData := make([]int, len(intData))
		copy(sortedData, intData)
		sort.Ints(sortedData)

		// Run the select function
		lo, hi := selectFunc(sort.IntSlice(intData), int(k))

		// Verify the range
		if hi-lo != int(k) {
			t.Fatalf("%s(n=%d, k=%d, %v) got [%d:%d], want length %d", name, n, k, intData, lo, hi, k)
		}

		// Verify the selected elements are all <= the k-th element
		kth := sortedData[k-1]
		topk := intData[lo:hi]
		for _, elem := range topk {
			if elem > kth {
				t.Fatalf("%s(n=%d, k=%d) element %d > %d\ndata:  %v\nsorted: %v\ntopk: %v", name, n, k, elem, kth, intData, sortedData, topk)
			}
		}
	})
}
