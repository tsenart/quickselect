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
	"fmt"
	"math/bits"
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
A type, typically a collection, which satisfies quickselect.Interface can be
used as data in the QuickSelect method. The interface is the same as the
interface required by Go's canonical sorting package (sort.Interface).

Note that the methods require that the elements of the collection be enumerated
by an integer index.
*/
type Interface interface {
	// Len is the number of elements in the collection
	Len() int
	// Less reports whether the element with
	// index i should sort before the element with index j
	Less(i, j int) bool
	// Swap swaps the order of elements i and j
	Swap(i, j int)
}

type reverse struct {
	// This embedded Interface permits Reverse to use the methods of
	// another Interface implementation.
	Interface
}

// Less returns the opposite of the embedded implementation's Less method.
func (r reverse) Less(i, j int) bool {
	return r.Interface.Less(j, i)
}

func Reverse(data Interface) Interface {
	return &reverse{data}
}

// The IntSlice type attaches the QuickSelect interface to an array of ints. It
// implements Interface so that you can call QuickSelect(k) on any IntSlice.
type IntSlice []int

func (t IntSlice) Len() int {
	return len(t)
}

func (t IntSlice) Less(i, j int) bool {
	return t[i] < t[j]
}

func (t IntSlice) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

// QuickSelect(k) mutates the IntSlice so that the first k elements in the
// IntSlice are the k smallest elements in the slice. This is a convenience
// method for QuickSelect
func (t IntSlice) QuickSelect(k int) error {
	return QuickSelect(t, k)
}

// The Float64Slice type attaches the QuickSelect interface to an array of
// float64s. It implements Interface so that you can call QuickSelect(k) on any
// Float64Slice.
type Float64Slice []float64

func (t Float64Slice) Len() int {
	return len(t)
}

func (t Float64Slice) Less(i, j int) bool {
	return t[i] < t[j] || isNaN(t[i]) && !isNaN(t[j])
}

func (t Float64Slice) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

// QuickSelect(k) mutates the Float64Slice so that the first k elements in the
// Float64Slice are the k smallest elements in the slice. This is a convenience
// method for QuickSelect
func (t Float64Slice) QuickSelect(k int) error {
	return QuickSelect(t, k)
}

// The StringSlice type attaches the QuickSelect interface to an array of
// float64s. It implements Interface so that you can call QuickSelect(k) on any
// StringSlice.
type StringSlice []string

func (t StringSlice) Len() int {
	return len(t)
}

func (t StringSlice) Less(i, j int) bool {
	return t[i] < t[j]
}

func (t StringSlice) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

// QuickSelect(k) mutates the StringSlice so that the first k elements in the
// StringSlice are the k smallest elements in the slice. This is a convenience
// method for QuickSelect
func (t StringSlice) QuickSelect(k int) error {
	return QuickSelect(t, k)
}

// isNaN is a copy of math.IsNaN to avoid a dependency on the math package.
func isNaN(f float64) bool {
	return f != f
}

/*
Helper function that does all of the work for QuickSelect. This implements
Hoare's Selection Algorithm which finds the smallest k elements in an interface
in expected O(n) time.

The algorithm works by finding a random pivot element, and making sure all the
elements to the left are less than the pivot element and vice versa for
elements on the right. Recursing on this solves the selection algorithm.
*/
func randomizedSelectionFinding(data Interface, low, high, k int) {
	var pivotIndex int

	for {
		if low >= high {
			return
		} else if high-low <= partitionThreshold {
			insertionSort(data, low, high+1)
			return
		}

		pivotIndex = choosePivot(data, low, high)
		lt, gt := partition3Way(data, low, high, pivotIndex)

		if k < lt {
			high = lt - 1
		} else if k > gt {
			low = gt + 1
		} else {
			return
		}
	}
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

// findMin finds the index of the minimum element in the data.
func findMinimum(data Interface, length int) int {
	minIndex := 0
	for i := 1; i < length; i++ {
		if data.Less(i, minIndex) {
			minIndex = i
		}
	}
	return minIndex
}

// Insertion sort
func insertionSort(data Interface, a, b int) {
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
func naiveSelectionFinding(data Interface, k int) {
	smallestIndices := make([]int, k)
	for i := 0; i < k; i++ {
		smallestIndices[i] = i
	}
	resetLargestIndex(smallestIndices, data)

	length := data.Len()
	for i := k; i < length; i++ {
		if data.Less(i, smallestIndices[k-1]) {
			smallestIndices[k-1] = i
			resetLargestIndex(smallestIndices, data)
		}
	}

	insertionSort(IntSlice(smallestIndices), 0, k)
	for i := 0; i < k; i++ {
		data.Swap(i, smallestIndices[i])
	}
}

/*
Takes the largest index in `indices` according to the data Interface and places
it at the end of the indices array.
*/
func resetLargestIndex(indices []int, data Interface) {
	var largestIndex = 0
	var currentLargest = indices[0]

	for i := 1; i < len(indices); i++ {
		if data.Less(currentLargest, indices[i]) {
			largestIndex = i
			currentLargest = indices[i]
		}
	}

	indices[len(indices)-1], indices[largestIndex] = indices[largestIndex], indices[len(indices)-1]
}

/*
Helper function for the selection algorithm. Returns the partitionIndex.

It goes through all elements between low and high and makes sure that the
elements in the range [low, partitionIndex) are less than the element that was
originally in the pivotIndex and that the elements in the range
[paritionIndex + 1, high] are greater than the element originally in the
pivotIndex.
*/
func partition(data Interface, a, b, pivot int) int {
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

func heapInit(data Interface, heap []int) {
	// Heapify process
	n := len(heap)
	for i := n/2 - 1; i >= 0; i-- {
		heapDown(data, heap, i, n)
	}
}

func heapDown(data Interface, heap []int, i, n int) {
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && data.Less(heap[j1], heap[j2]) {
			j = j2 // right child
		}
		if !data.Less(heap[i], heap[j]) {
			break
		}
		heap[i], heap[j] = heap[j], heap[i]
		i = j
	}
}

/*
This method implements the heap strategy for selecting the smallest k elements.
It keeps a max-heap of the smallest k elements seen so far as we iterate over
all of the elements. It adds a new element and pops the largest element.
*/
func heapSelectionFinding(data Interface, k int) {
	heap := make([]int, k)
	for i := 0; i < k; i++ {
		heap[i] = i
	}
	heapInit(data, heap)

	length := data.Len()
	for i := k; i < length; i++ {
		if data.Less(i, heap[0]) {
			heap[0] = i
			heapDown(data, heap, 0, k)
		}
	}

	insertionSort(IntSlice(heap), 0, k)
	for i := 0; i < k; i++ {
		data.Swap(i, heap[i])
	}
}

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
func QuickSelect(data Interface, k int) error {
	length := data.Len()
	if k < 1 || k > length {
		return fmt.Errorf("The specified index '%d' is outside of the data's range of indices [0,%d)", k, length)
	}

	// if k == 1 {
	// 	minIdx := findMinimum(data, length)
	// 	data.Swap(0, minIdx)
	// 	return nil
	// }

	pdqQuickSelect(data, 0, length-1, k-1, bits.Len(uint(data.Len())))

	return nil
}

func pdqQuickSelect(data Interface, a, b, k, limit int) {
	const maxInsertion = 12

	var (
		wasBalanced    = true
		wasPartitioned = true
	)

	for {
		length := b - a

		if length <= maxInsertion {
			insertionSort_func(data, a, b+1)
			return
		}

		// Fall back to heapsort if too many bad choices were made.
		if limit == 0 {
			heapSort_func(data, a, b+1)
			return
		}

		// Break patterns if the last partitioning was imbalanced
		if !wasBalanced {
			breakPatterns_func(data, a, b+1)
			limit--
		}

		pivot, hint := choosePivot_func(data, a, b+1)
		if hint == decreasingHint {
			reverseRange_func(data, a, b+1)
			pivot = (b - a) - (pivot - a)
			hint = increasingHint
		}

		// Check if the slice is likely already sorted
		if wasBalanced && wasPartitioned && hint == increasingHint {
			if partialInsertionSort_func(data, a, b+1) {
				return
			}
		}

		// Handle many duplicate elements
		if a > 0 && !data.Less(a-1, pivot) {
			mid := partitionEqual_func(data, a, b+1, pivot)
			if k < mid-a {
				b = mid - 1
			} else if k >= mid-a {
				k -= mid - a
				a = mid
			} else {
				return // k is in the range of equal elements
			}
			continue
		}

		newPivot, alreadyPartitioned := partition_func(data, a, b+1, pivot)
		wasPartitioned = alreadyPartitioned

		leftLen, rightLen := newPivot-a, b-newPivot
		balanceThreshold := length / 8

		if k == newPivot {
			return
		} else if k < newPivot {
			wasBalanced = leftLen >= balanceThreshold
			b = newPivot - 1
		} else {
			wasBalanced = rightLen >= balanceThreshold
			a = newPivot + 1
		}

		limit--
	}
}

// IntQuickSelect mutates the data so that the first k elements in the int
// slice are the k smallest elements in the slice. This is a convenience
// method for QuickSelect on int slices.
func IntQuickSelect(data []int, k int) error {
	return QuickSelect(IntSlice(data), k)
}

// Float64Select mutates the data so that the first k elements in the float64
// slice are the k smallest elements in the slice. This is a convenience
// method for QuickSelect on float64 slices.
func Float64QuickSelect(data []float64, k int) error {
	return QuickSelect(Float64Slice(data), k)
}

// StringQuickSelect mutates the data so that the first k elements in the string
// slice are the k smallest elements in the slice. This is a convenience
// method for QuickSelect on string slices.
func StringQuickSelect(data []string, k int) error {
	return QuickSelect(StringSlice(data), k)
}
