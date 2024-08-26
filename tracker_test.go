package emaildomainstats_test

import (
	"fmt"
	emaildomainstats "github.com/fcuenca/go-tech-test"
	"sync"
	"testing"
)

// customLocker is a wrapper around sync.Mutex that counts lock acquisitions
type customLocker struct {
	mu         sync.Mutex
	LockCount  int
	LockNotify chan struct{}
}

func newCustomLocker() *customLocker {
	return &customLocker{
		mu:         sync.Mutex{},
		LockCount:  0,
		LockNotify: make(chan struct{}, 100),
	}
}

func (c *customLocker) Lock() {
	c.mu.Lock()
	c.LockCount++
	c.LockNotify <- struct{}{}
}

func (c *customLocker) Unlock() {
	c.mu.Unlock()
}

func Test_DomainStatTracker(t *testing.T) {
	type tc struct {
		name         string
		setup        func(tracker *emaildomainstats.DomainStatTracker, locker *customLocker)
		expectedLock int
		expected     map[string]int64
	}

	testCases := []tc{
		{
			name: "Inserting two new domains concurrently should lock twice",
			setup: func(tracker *emaildomainstats.DomainStatTracker, locker *customLocker) {
				var wg sync.WaitGroup
				wg.Add(2)

				go func() {
					defer wg.Done()
					tracker.Add("new1.com")
				}()

				go func() {
					defer wg.Done()
					tracker.Add("new2.com")
				}()

				wg.Wait()
			},
			expectedLock: 2,
			expected:     map[string]int64{"new1.com": 1, "new2.com": 1},
		},
		{
			name: "Incrementing two existing domains concurrently should not lock",
			setup: func(tracker *emaildomainstats.DomainStatTracker, locker *customLocker) {
				tracker.Add("example.com")
				tracker.Add("test.com")
				locker.LockCount = 0

				var wg sync.WaitGroup
				wg.Add(2)

				go func() {
					defer wg.Done()
					tracker.Add("example.com")
				}()

				go func() {
					defer wg.Done()
					tracker.Add("test.com")
				}()

				wg.Wait()
			},
			expectedLock: 0,
			expected:     map[string]int64{"example.com": 2, "test.com": 2},
		},
		{
			name: "Incrementing the same existing domain 50 times should not lock",
			setup: func(tracker *emaildomainstats.DomainStatTracker, locker *customLocker) {
				tracker.Add("example.com")
				locker.LockCount = 0

				var wg sync.WaitGroup
				wg.Add(50)

				for i := 0; i < 50; i++ {
					go func() {
						defer wg.Done()
						tracker.Add("example.com")
					}()
				}
				wg.Wait()
			},
			expectedLock: 0,
			expected:     map[string]int64{"example.com": 51},
		},
		{
			name: "Inserting 10 new domains concurrently in non-alphabetical order",
			setup: func(tracker *emaildomainstats.DomainStatTracker, locker *customLocker) {
				var wg sync.WaitGroup
				wg.Add(10)

				for i := 0; i < 10; i++ {
					go func(i int) {
						defer wg.Done()
						tracker.Add(fmt.Sprintf("domain%02d.com", (i*7)%10))
					}(i)
				}

				wg.Wait()
			},
			expectedLock: 10,
			expected: map[string]int64{
				"domain00.com": 1, "domain01.com": 1, "domain02.com": 1, "domain03.com": 1, "domain04.com": 1,
				"domain05.com": 1, "domain06.com": 1, "domain07.com": 1, "domain08.com": 1, "domain09.com": 1,
			},
		},
		{
			name: "Inserting 10 new domains and 10 existing concurrently in non-alphabetical order",
			setup: func(tracker *emaildomainstats.DomainStatTracker, locker *customLocker) {
				var wg sync.WaitGroup
				wg.Add(20)

				// Adds 10 non-existing domains
				for i := 0; i < 10; i++ {
					go func(i int) {
						defer wg.Done()
						tracker.Add(fmt.Sprintf("domain%02d.com", (i*7)%10))
					}(i)
				}

				// Adds 10 times the same existing domain
				for i := 0; i < 10; i++ {
					go func(i int) {
						defer wg.Done()
						tracker.Add("domain04.com")
					}(i)
				}

				wg.Wait()
			},
			expectedLock: 10,
			expected: map[string]int64{
				"domain00.com": 1, "domain01.com": 1, "domain02.com": 1, "domain03.com": 1, "domain04.com": 11,
				"domain05.com": 1, "domain06.com": 1, "domain07.com": 1, "domain08.com": 1, "domain09.com": 1,
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			locker := newCustomLocker()
			tracker := emaildomainstats.NewDomainStatTracker(emaildomainstats.WithLocker(locker))

			tt.setup(tracker, locker)

			if locker.LockCount != tt.expectedLock {
				t.Errorf("Expected LockCount %d, got %d", tt.expectedLock, locker.LockCount)
			}

			result := tracker.GetSorted()
			if len(result) != len(tt.expected) {
				t.Errorf("Expected %d domains, got %d", len(tt.expected), len(result))
			}

			for _, domain := range result {
				expectedCount, exists := tt.expected[domain.Domain]
				if !exists {
					t.Errorf("Unexpected domain %s", domain.Domain)
				} else if domain.Count() != expectedCount {
					t.Errorf("Expected count %d for domain %s, got %d", expectedCount, domain.Domain, domain.Count())
				}
			}
		})
	}
}
