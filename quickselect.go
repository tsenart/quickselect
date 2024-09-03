/*
The quickselect package provides primitives for finding the smallest k elements
in slices and user-defined collections. The primitives used in the package are
modeled off of the standard sort library for Go. Quickselect uses Hoare's
Selection Algorithm which finds the smallest k elements in expected O(n) time,
and is thus an asymptotically optimal algorithm (and is faster than sorting or
heap implementations).
*/
package quickselect

import (
	"math"
	"slices"
	"sort"
)

const (
	partitionThreshold            = 8
	naiveSelectionLengthThreshold = 100
	naiveSelectionThreshold       = 10
	heapSelectionKRatio           = 0.001
	heapSelectionThreshold        = 1e3
)

/*
QuickSelect swaps elements in the data provided so that the first k elements
(i.e. the elements occuping indices 0, 1, ..., k-1) are the smallest k elements
in the data.

QuickSelect implements Hoare's Selection Algorithm and runs in O(n) time, so it
is asymptotically faster than sorting or other heap-like implementations for
finding the smallest k elements in a data structure.

Note that k must be in the range [0, data.Len()), otherwise the QuickSelect
method will raise an error.
*/
func QuickSelect(data sort.Interface, k int) (lo, hi int) {
	// length := data.Len()
	// if k <= 0 || length <= 0 {
	// 	return 0, 0
	// }

	// if k >= length {
	// 	return 0, length
	// }

	// kRatio := float64(k) / float64(length)
	// if length <= naiveSelectionLengthThreshold && k <= naiveSelectionThreshold {
	// 	lo, hi = naiveSelect(data, k)
	// } else if kRatio <= heapSelectionKRatio && k <= heapSelectionThreshold {
	return heapSelect(data, k)
	// }

	// return quickSelect(data, k)
}

/*
Helper function that does all of the work for QuickSelect. This implements
Hoare's Selection Algorithm which finds the smallest k elements in an interface
in expected O(n) time.

The algorithm works by finding a random pivot element, and making sure all the
elements to the left are less than the pivot element and vice versa for
elements on the right. Recursing on this solves the selection algorithm.
*/
// QuickSelect finds the kth smallest element in data[a:b] and ensures that
// the k smallest elements are placed at the beginning of the slice.
// It returns the index range containing the kth smallest element.
func quickSelect(data sort.Interface, k int) (lo, hi int) {
	length := data.Len()
	if k <= 0 || length <= 0 {
		return 0, 0
	}

	if k >= length {
		return 0, length
	}

	s := sortedness(data)
	if s < 0 {
		data = sort.Reverse(data)
	}

	if k == 1 {
		lo = findMinimum(data, length)
		return lo, lo + 1
	}

	a, b := 0, length-1
	k += a

	for b > a {
		if b-a <= partitionThreshold {
			insertionSort(data, a, b+1)
			return a, k
		}

		pivot := choosePivot(data, a, b)
		lt, gt := partition3Way(data, a, b, pivot)

		if k < lt {
			b = lt - 1
		} else if k > gt {
			a = gt + 1
		} else {
			return a, k
		}
	}

	return a, a
}

// findMin finds the index of the minimum element in the data.
func findMinimum(data sort.Interface, length int) int {
	minIndex := 0
	for i := 1; i < length; i++ {
		if data.Less(i, minIndex) {
			minIndex = i
		}
	}
	return minIndex
}

// partition3Way partitions the slice data[a:b] around the pivot element.
// It returns two indices: lt and gt.
// - All elements in data[a:lt] are less than the pivot.
// - All elements in data[lt:gt+1] are equal to the pivot.
// - All elements in data[gt+1:b] are greater than the pivot.
func partition3Way(data sort.Interface, a, b, pivot int) (lt, gt int) {
	data.Swap(a, pivot)
	lt, gt = a, b-1
	i := a + 1

	for i <= gt {
		if data.Less(i, a) {
			data.Swap(lt, i)
			lt++
			i++
		} else if data.Less(a, i) {
			data.Swap(i, gt)
			gt--
		} else {
			i++
		}
	}

	// Handle the case where all elements are equal
	if lt == a && gt == b-1 {
		return a, b - 1
	}

	data.Swap(a, lt)
	return lt, gt
}

// Insertion sort
func insertionSort(data sort.Interface, a, b int) {
	for i := a + 1; i < b; i++ {
		for j := i; j > a && data.Less(j, j-1); j-- {
			data.Swap(j, j-1)
		}
	}
}

/*
This method does a run over all of the data keeps a list of the k smallest
indices that it has seen so far. At the end, it swaps those k elements and
moves them to the front.
*/
func naiveSelect(data sort.Interface, k int) (lo, hi int) {
	length := data.Len()
	if k <= 0 || length <= 0 {
		return 0, 0
	}

	if k == 1 {
		lo = findMinimum(data, length)
		return lo, lo + 1
	}

	if k >= length {
		return 0, length
	}

	// Use the first k elements as our initial "smallest" set
	largestIdx := 0
	for i := 1; i < k; i++ {
		if data.Less(largestIdx, i) {
			largestIdx = i
		}
	}

	// Compare the rest of the elements
	for i := k; i < length; i++ {
		if data.Less(i, largestIdx) {
			// Found a smaller element, replace the largest in our set
			data.Swap(i, largestIdx)

			// Find the new largest in our set
			largestIdx = 0
			for j := 1; j < k; j++ {
				if data.Less(largestIdx, j) {
					largestIdx = j
				}
			}
		}
	}

	// The k smallest elements are now in the first k positions
	return 0, k
}

/*
Helper function for the selection algorithm. Returns the partitionIndex.

It goes through all elements between low and high and makes sure that the
elements in the range [low, partitionIndex) are less than the element that was
originally in the pivotIndex and that the elements in the range
[paritionIndex + 1, high] are greater than the element originally in the
pivotIndex.
*/
func partition(data sort.Interface, a, b, pivot int) int {
	data.Swap(a, pivot)
	i, j := a+1, b-1 // i and j are inclusive of the elements remaining to be partitioned

	for i <= j && data.Less(i, a) {
		i++
	}
	for i <= j && !data.Less(j, a) {
		j--
	}
	if i > j {
		data.Swap(j, a)
		return j
	}
	data.Swap(i, j)
	i++
	j--

	for {
		for i <= j && data.Less(i, a) {
			i++
		}
		for i <= j && !data.Less(j, a) {
			j--
		}
		if i > j {
			break
		}
		data.Swap(i, j)
		i++
		j--
	}
	data.Swap(j, a)
	return j
}

// choosePivot chooses a pivot in data[a:b].
//
// [0,8): chooses a static pivot.
// [8,shortestNinther): uses the simple median-of-three method.
// [shortestNinther,∞): uses the Tukey ninther method.
func choosePivot(data sort.Interface, a, b int) (pivot int) {
	const (
		shortestNinther = 50
		maxSwaps        = 4 * 3
	)

	l := b - a

	var (
		swaps int
		i     = a + l/4*1
		j     = a + l/4*2
		k     = a + l/4*3
	)

	if l >= 8 {
		if l >= shortestNinther {
			// Tukey ninther method, the idea came from Rust's implementation.
			i = medianAdjacent(data, i, &swaps)
			j = medianAdjacent(data, j, &swaps)
			k = medianAdjacent(data, k, &swaps)
		}
		// Find the median among i, j, k and stores it into j.
		j = median(data, i, j, k, &swaps)
	}

	switch swaps {
	case 0:
		return j
	case maxSwaps:
		return j
	default:
		return j
	}
}

// order2 returns x,y where data[x] <= data[y], where x,y=a,b or x,y=b,a.
func order2(data sort.Interface, a, b int, swaps *int) (int, int) {
	if data.Less(b, a) {
		*swaps++
		return b, a
	}
	return a, b
}

// median returns x where data[x] is the median of data[a],data[b],data[c], where x is a, b, or c.
func median(data sort.Interface, a, b, c int, swaps *int) int {
	a, b = order2(data, a, b, swaps)
	b, c = order2(data, b, c, swaps)
	a, b = order2(data, a, b, swaps)
	return b
}

// medianAdjacent finds the median of data[a - 1], data[a], data[a + 1] and stores the index into a.
func medianAdjacent(data sort.Interface, a int, swaps *int) int {
	return median(data, a-1, a, a+1, swaps)
}

/*
This method implements the heap strategy for selecting the smallest k elements.
It keeps a max-heap of the smallest k elements seen so far as we iterate over
all of the elements. It adds a new element and pops the largest element.
*/
func heapSelect(data sort.Interface, k int) (lo, hi int) {
	n := data.Len()
	if k <= 0 || n <= 0 {
		return 0, 0
	}

	if k == 1 {
		lo = findMinimum(data, n)
		return lo, lo + 1
	}

	if k >= n {
		return 0, n
	}

	heap := make([]int, k)
	for i := range heap {
		heap[i] = i
	}

	s := sortedness(data)
	if s >= 0 {
		heapInit(data, heap)

		const root = 0
		for i := k; i < n; i++ {
			if data.Less(i, root) {
				heap[root] = i
				heapDown(data, heap, root, k)
			}
		}
	} else {
		revHeapInit(data, heap)

		root := n - 1
		for i := n - k - 1; i < n; i++ {
			if data.Less(i, root) {
				heap[root] = i
				revHeapDown(data, heap, root, k)
			}
		}

		// Now reverse the last k elements
		for i, j := n-k, n-1; i < j; i, j = i+1, j-1 {
			data.Swap(i, j)
		}
	}

	slices.Sort(heap)
	for i := range heap {
		data.Swap(i, heap[i])
	}

	return 0, k
}

func heapInit(data sort.Interface, heap []int) {
	k := len(heap)
	for i := k/2 - 1; i >= 0; i-- {
		heapDown(data, heap, i, k)
	}
}

func heapDown(data sort.Interface, heap []int, i0, k int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= k || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < k && data.Less(heap[j2], heap[j1]) {
			j = j2 // = 2*i + 2  // right child
		}
		if !data.Less(heap[j], heap[i]) {
			break
		}
		heap[i], heap[j] = heap[j], heap[i]
		i = j
	}
	return i > i0
}

func revHeapInit(data sort.Interface, heap []int) {
	n := data.Len()
	k := len(heap)
	for i := n - k/2 - 1; i < n; i++ {
		revHeapDown(data, heap, i, k)
	}
}

func revHeapDown(data sort.Interface, heap []int, i0, k int) bool {
	n := data.Len()
	hi := n - k
	i := i0
	for {
		left := n - 2*(n-i)
		if left < hi {
			break
		}
		j := left
		right := left - 1
		if right >= 0 && data.Less(heap[right], heap[left]) {
			j = right
		}
		if !data.Less(heap[j], heap[i]) {
			break
		}
		heap[i], heap[j] = heap[j], heap[i]
		i = j
	}
	return i < i0
}

// sortedness estimates the sortedness of a slice and its direction.
// The return value between -1.0 and 0.0 indicates a inverted order,
// and 0.0 to 1.0 indicates desired order.
//
// Don't use this function if the array data has periodicity — since it
// uses systematic sampling, it will not be able to sample uniformly.
func sortedness(data sort.Interface) float32 {
	ln := data.Len()

	switch ln {
	case 0, 1:
		return 1.0
	case 2:
		if data.Less(0, 1) {
			return 1.0
		} else if data.Less(1, 0) {
			return -1.0
		}
	}

	sampleSize := sampleSize(ln, 0.05)
	ordered, inverted := 0, 0

	// Determine the stride length based on the sample size and array length
	stride := ln / sampleSize
	if stride < 1 {
		stride = 1 // Ensure at least one element per stride
	}

	// Linearly iterate over the array with the determined stride
	for i := 0; i < ln-1; i += stride {
		j := i + 1
		if data.Less(i, j) {
			ordered++
		} else if data.Less(j, i) {
			inverted++
		}
	}

	// Adjust the count based on the actual number of comparisons made
	actualSamples := (ln - 1) / stride

	// Calculate sortedness by dividing ordered pairs by total pairs
	return float32(ordered-inverted) / float32(actualSamples)
}

// sampleSize calculates the required sample size for a given population size using Yamane's formula.
//
// Parameters:
// - popSize (int): Total number of individuals in the population.
// - marginOfError (float64): Desired margin of error (e.g., 0.05 for 5%).
//
// Examples: (10, 0.05) -> 10, (50, 0.05) -> 45, (100, 0.05) -> 80, (10000, 0.05) -> 385, (10M, 0.05) -> 400
// Ref: https://www.tenato.com/market-research/what-is-the-ideal-sample-size-for-a-survey/
func sampleSize(popSize int, marginOfError float64) int {
	// Calculate the sample size using Yamane's formula with float64 arithmetic
	n := float64(popSize) / (1.0 + float64(popSize)*(marginOfError*marginOfError))
	// Round up to the nearest whole number and ensure it's not larger than the population size
	return min(int(math.Ceil(n)), popSize)
}
