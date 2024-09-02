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
	"math"
	"math/rand/v2"
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

		pivotIndex = rand.IntN(high+1-low) + low
		pivotIndex = partition(data, low, high, pivotIndex)

		if k < pivotIndex {
			high = pivotIndex - 1
		} else if k > pivotIndex {
			low = pivotIndex + 1
		} else {
			return
		}
	}
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
func partition(data Interface, low, high, pivotIndex int) int {
	partitionIndex := low
	data.Swap(pivotIndex, high)
	for i := low; i < high; i++ {
		if data.Less(i, high) {
			data.Swap(i, partitionIndex)
			partitionIndex++
		}
	}
	data.Swap(partitionIndex, high)
	return partitionIndex
}

func heapify(h Interface, n int) {
	for i := n/2 - 1; i >= 0; i-- {
		down(h, i, n)
	}
}

func fix(h Interface, n int, i int) {
	if !down(h, i, n) {
		up(h, i)
	}
}

func up(h Interface, j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.Less(i, j) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func down(h Interface, i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h.Less(j1, j2) {
			j = j2 // = 2*i + 2  // right child
		}
		if !h.Less(i, j) {
			break
		}
		h.Swap(i, j)
		i = j
	}
	return i > i0
}

/*
This method implements the heap strategy for selecting the smallest k elements.
It keeps a max-heap of the smallest k elements seen so far as we iterate over
all of the elements. It adds a new element and pops the largest element.
*/
func heapSelectionFinding(data Interface, k int) {
	l := data.Len()
	if k >= l {
		return
	}

	s := sortedness(data)
	if s < 0 {
		last := l - 1
		for i := 0; i < k; i++ {
			data.Swap(i, last-i)
		}
	}

	heapify(data, k)

	// data[:k] is now in a heap order but such that data[0] is the max element.
	// We now consider each data[k:] and if its less than data[0] we pop data[0]
	// and swap it in and restore the heap invariants
	for i := k; i < l; i++ {
		if data.Less(i, 0) {
			data.Swap(i, 0)
			fix(data, k, 0)
		}
	}
}

type backwards struct {
	sort.Interface
	lastIdx int // cached
}

func (b backwards) Less(i, j int) bool {
	return b.Interface.Less(b.lastIdx-i, b.lastIdx-j)
}

func (b backwards) Swap(i, j int) {
	b.Interface.Swap(b.lastIdx-i, b.lastIdx-j)
}

// sortedness estimates the sortedness of a slice and its direction.
// The return value between -1.0 and 0.0 indicates a inverted order,
// and 0.0 to 1.0 indicates desired order.
//
// Don't use this function if the array data has periodicity — since it
// uses systematic sampling, it will not be able to sample uniformly.
func sortedness(data Interface) float32 {
	ln := data.Len()

	switch ln {
	case 0, 1:
		return 1.0
	case 2:
		return 1.0
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

	kRatio := float64(k) / float64(length)
	if length <= naiveSelectionLengthThreshold && k <= naiveSelectionThreshold {
		naiveSelectionFinding(data, k)
	} else if kRatio <= heapSelectionKRatio && k <= heapSelectionThreshold {
		heapSelectionFinding(data, k)
	} else {
		randomizedSelectionFinding(data, 0, length-1, k)
	}

	return nil
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
