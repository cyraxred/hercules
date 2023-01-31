package leaves

import (
	"github.com/cyraxred/hercules/internal/burndown"
	"io"
	"sort"
	"time"
)

// BurndownResult carries the result of running BurndownAnalysis - it is returned by
// BurndownAnalysis.Finalize().
type BurndownResult struct {
	// [number of samples][number of bands]
	// The number of samples depends on Sampling: the less Sampling, the bigger the number.
	// The number of bands depends on Granularity: the less Granularity, the bigger the number.
	GlobalHistory burndown.DenseHistory
	// The key is a path inside the Git repository. The value's dimensions are the same as
	// in GlobalHistory.
	FileHistories map[string]burndown.DenseHistory
	// The key is a path inside the Git repository. The value is a mapping from developer indexes
	// (see reversedPeopleDict) and the owned line numbers. Their sum equals to the total number of
	// lines in the file.
	FileOwnership map[string]map[int]int
	// [number of people][number of samples][number of bands]
	PeopleHistories []burndown.DenseHistory
	// [number of people][number of people + 2]
	// The first element is the total number of lines added by the author.
	// The second element is the number of removals by unidentified authors (outside reversedPeopleDict).
	// The rest of the elements are equal the number of line removals by the corresponding
	// authors in reversedPeopleDict: 2 -> 0, 3 -> 1, etc.
	PeopleMatrix burndown.DenseHistory

	// The following members are private.

	// reversedPeopleDict is borrowed from IdentityDetector and becomes available after
	// Pipeline.Initialize(facts map[string]interface{}). Thus it can be obtained via
	// facts[FactIdentityDetectorReversedPeopleDict].
	reversedPeopleDict []string
	// TickSize references TicksSinceStart.TickSize
	tickSize time.Duration
	// sampling and granularity are copied from BurndownAnalysis and stored for service purposes
	// such as merging several results together.
	sampling    int
	granularity int
}

// GetTickSize returns the tick size used to generate this burndown analysis result.
func (br BurndownResult) GetTickSize() time.Duration {
	return br.tickSize
}

// GetIdentities returns the list of developer identities used to generate this burndown analysis result.
// The format is |-joined keys, see internals/plumbing/identity for details.
func (br BurndownResult) GetIdentities() []string {
	return br.reversedPeopleDict
}

type sparseHistoryEntry struct {
	deltas map[int]int64
}

func newSparseHistoryEntry() sparseHistoryEntry {
	return sparseHistoryEntry{
		deltas: map[int]int64{},
	}
}

type sparseHistory map[int]sparseHistoryEntry

func (p sparseHistory) updateDelta(prevTick, curTick int, delta int) {
	currentHistory, ok := p[curTick]
	if !ok {
		currentHistory = newSparseHistoryEntry()
		p[curTick] = currentHistory
	}
	currentHistory.deltas[prevTick] += int64(delta)
}

func sortedKeys(m map[string]burndown.DenseHistory) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func checkClose(c io.Closer) {
	if err := c.Close(); err != nil {
		panic(err)
	}
}
