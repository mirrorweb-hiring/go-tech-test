<p align="center">
  <img src="public/mw-logo-only.svg" alt="Logo" height=170>
</p>

# Go Tech Test

## 🎛️ Overview

Welcome to the MirrorWeb Go Tech Test! We'd like to see how you approach building out some new functionality suitable for a production setting.

This repository is a fairly sparse place but should contain everything you need to get started. Open up emaildomainstats.go for instructions.

## Notes

- Please endevour to use the the standard library only.
- This tech test is not intended to take a significant amount of your time up so please factor this into your solution. If you have ideas for how this could be expanded upon, improved or compromises you decided on during the test make a note of them. They can be good to discuss in your interview.

## Submitting your work

To complete the tech test you will need to clone down this GitHub repo, create a feature branch from main and do all of your work from this branch, create a single PR with all your changes back into main. When you’re ready, share your submission back with us, ideally via a private Github repository with the PR open so we can schedule it for review. We review submissions as quickly as possible. You’ll hear back from us no matter what.

Remember, we are not timing you and there is no deadline. Work at your own pace. Reach out if you're stuck or have any questions. We want you to succeed and are here to help! We won't give you much in the way of hints, but we can at least help you orient yourself about the challenge!


# DomainStatTracker Implementation Considerations

## Data Structure Choice

We've chosen to use a combination of a heap and a map for our `DomainStatTracker`:

1. **Heap**: Used to maintain domains sorted alphabetically.
    - Push: O(log n)
    - Pop: O(log n)
    - Peek: O(1)
    - Sort: O(n log n)

2. **Map**: Used for fast domain lookup.
    - Lookup: O(1)

### Rationale
- The heap allows for efficient sorting and retrieval of domains in alphabetical order.
- The map provides O(1) lookup time, which is crucial for fast increments when processing large numbers of email domains.

### Trade-offs
- The solution prioritizes fast increments and sorting over memory efficiency.
- The data structure can only be read once by heap design.

## Concurrency and Thread Safety

The solution is designed to be concurrent-safe:

1. **Atomic Operations**: `atomic.Int64` is used for the `Count` within `DomainStat` to ensure thread-safe increments without full locking.

2. **Sync.Map**: Used instead of a regular map to provide concurrent-safe access to the domain statistics.

3. **Mutex**: A mutex (`sync.Locker`) is used to protect heap operations (push/pop) which are not inherently thread-safe. 

### Considerations
- The use of `sync.Map` and atomic operations allows for high concurrency during domain count increments.
- The mutex may become a bottleneck if many new domains are added simultaneously, as it locks the entire heap for insertions. Worse case scenario is when all domains are different.
- Domains such as @fran.com and @fran+1.com are considered different
- CSV contains a header
- Domains like "fran.cuenca"@gmail.com are not valid
- Parser is implemented using the mail package (RFC 5322)

## Performance

1. **Increment Operation**:
    - Best case (existing domain): O(1) - only atomic increment
    - Worst case (new domain): O(log n) - heap insertion

2. **GetSorted Operation**: O(n log n) - popping all elements from the heap

## Limitations and Potential Improvements

1. **Single-Use Sorting**: The current `GetSorted` method destroys the heap. If multiple sorted retrievals are needed, consider:
    - Implementing a non-destructive sorting method
    - Using a self-balancing binary search tree instead of a heap

2. **Memory Usage**: For very large datasets, consider:
    - Using a temp disk-based storage
   