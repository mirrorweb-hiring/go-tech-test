package emaildomainstats

import (
	"container/heap"
	"strings"
	"sync"
	"sync/atomic"
)

// DomainStat represents statistics for a single domain
type DomainStat struct {
	Domain string
	count  atomic.Int64
	index  int // heap index
}

// Count retrieves the domain total count
func (d *DomainStat) Count() int64 {
	return d.count.Load()
}

// StatHeap is a min-heap of DomainStat pointers, ordered alphabetically by Domain
type StatHeap []*DomainStat

// Len returns the number of elements in the heap
func (h StatHeap) Len() int { return len(h) }

// Less defines the ordering of elements in the heap
func (h StatHeap) Less(i, j int) bool { return strings.Compare(h[i].Domain, h[j].Domain) < 0 }

// Swap exchanges the elements with indexes i and j
func (h StatHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

// Push adds an element to the heap and maintains the heap invariant
func (h *StatHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*DomainStat)
	item.index = n
	*h = append(*h, item)
}

// Pop removes and returns the min element from the heap
func (h *StatHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[0 : n-1]
	return item
}

// DomainStatTracker is a concurrent-safe data structure that tracks domain statistics
type DomainStatTracker struct {
	heap  *StatHeap
	cache sync.Map
	mu    sync.Locker
}

// Option is a function that configures the DomainStatTracker
type Option func(*DomainStatTracker)

// WithLocker sets a custom locker for the DomainStatTracker
func WithLocker(locker sync.Locker) Option {
	return func(tracker *DomainStatTracker) {
		tracker.mu = locker
	}
}

// NewDomainStatTracker creates and initializes a new DomainStatTracker
func NewDomainStatTracker(opts ...Option) *DomainStatTracker {
	sh := &StatHeap{}
	heap.Init(sh)

	tracker := &DomainStatTracker{
		heap:  sh,
		cache: sync.Map{},
		mu:    &sync.Mutex{},
	}

	for _, opt := range opts {
		opt(tracker)
	}

	return tracker
}

// Add increments the counter for the given domain by 1.
// concurrent safe.
func (dst *DomainStatTracker) Add(domain string) {
	actual, loaded := dst.cache.LoadOrStore(domain, &DomainStat{Domain: domain})
	stat := actual.(*DomainStat)
	stat.count.Add(1)
	if !loaded {
		// New domain: needs to be added to heap (synchronized)
		dst.mu.Lock()
		heap.Push(dst.heap, stat)
		dst.mu.Unlock()
	}
}

// GetSorted returns a list of domains with their counts, sorted alphabetically by domain.
// This method has a time complexity of O(n log n) and space complexity of O(n),
// where n is the number of unique domains.
func (dst *DomainStatTracker) GetSorted() []*DomainStat {
	dst.mu.Lock()
	defer dst.mu.Unlock()

	result := make([]*DomainStat, 0, len(*dst.heap))
	for dst.heap.Len() > 0 {
		item := heap.Pop(dst.heap).(*DomainStat)
		result = append(result, item)
	}

	return result
}
