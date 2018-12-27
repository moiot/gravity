package sliding_window

import "github.com/juju/errors"

type windowItemHeap []int64

func (h windowItemHeap) Len() int           { return len(h) }
func (h windowItemHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h windowItemHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *windowItemHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(int64))
}

func (h *windowItemHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *windowItemHeap) SmallestItem() (int64, error) {
	if len(*h) > 0 {
		return (*h)[0], nil
	}

	return 0, errors.Errorf("empty heap")
}
