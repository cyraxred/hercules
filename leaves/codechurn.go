package leaves

import (
	"fmt"
	"github.com/cyraxred/hercules/internal/linehistory"
	"io"
	"time"

	"github.com/cyraxred/hercules/internal/core"
	items "github.com/cyraxred/hercules/internal/plumbing"
	"github.com/cyraxred/hercules/internal/plumbing/identity"
	"github.com/go-git/go-git/v5"
)

// CodeChurnAnalysis allows to gather the code churn statistics for a Git repository.
// It is a LeafPipelineItem.
type CodeChurnAnalysis struct {
	core.NoopMerger
	// Granularity sets the size of each band - the number of ticks it spans.
	// Smaller values provide better resolution but require more work and eat more
	// memory. 30 ticks is usually enough.
	Granularity int
	// Sampling sets how detailed is the statistic - the size of the interval in
	// ticks between consecutive measurements. It may not be greater than Granularity. Try 15 or 30.
	Sampling int

	// TrackFiles enables or disables the fine-grained per-file burndown analysis.
	// It does not change the project level burndown results.
	TrackFiles bool

	// PeopleNumber is the number of developers for which to collect the burndown stats. 0 disables it.
	PeopleNumber int

	// Repository points to the analysed Git repository struct from go-git.
	repository *git.Repository

	// TickSize indicates the size of each time granule: day, hour, week, etc.
	tickSize time.Duration
	// references IdentityDetector.ReversedPeopleDict
	reversedPeopleDict []string

	// code churns indexed by people
	codeChurns []personChurnStats

	fileResolver linehistory.FileIdResolver

	l core.Logger
}

type churnFileEntry struct {
	insertedLines int64
	deleteHistory map[ /* by person */ int]sparseHistory
}

type churnDeletedFileEntry struct {
	fileId    linehistory.FileId
	deletedAt int
	entry     churnFileEntry
}

type personChurnStats struct {
	files        map[linehistory.FileId]churnFileEntry
	deletedFiles []churnDeletedFileEntry
}

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (analyser *CodeChurnAnalysis) Name() string {
	return "CodeChurn"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (analyser *CodeChurnAnalysis) Provides() []string {
	return []string{}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (analyser *CodeChurnAnalysis) Requires() []string {
	return []string{linehistory.DependencyLineHistory, identity.DependencyAuthor}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (analyser *CodeChurnAnalysis) ListConfigurationOptions() []core.ConfigurationOption {
	return BurndownSharedOptions[:]
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (analyser *CodeChurnAnalysis) Configure(facts map[string]interface{}) error {
	if l, exists := facts[core.ConfigLogger].(core.Logger); exists {
		analyser.l = l
	} else {
		analyser.l = core.NewLogger()
	}

	if val, exists := facts[items.FactTickSize].(time.Duration); exists {
		analyser.tickSize = val
	}
	if val, exists := facts[ConfigBurndownGranularity].(int); exists {
		analyser.Granularity = val
	}
	if val, exists := facts[ConfigBurndownSampling].(int); exists {
		analyser.Sampling = val
	}
	if val, exists := facts[ConfigBurndownTrackFiles].(bool); exists {
		analyser.TrackFiles = val
	}
	if people, exists := facts[ConfigBurndownTrackPeople].(bool); people {
		if val, exists := facts[identity.FactIdentityDetectorPeopleCount].(int); exists {
			if val < 0 {
				return fmt.Errorf("PeopleNumber is negative: %d", val)
			}
			analyser.PeopleNumber = val
			analyser.reversedPeopleDict = facts[identity.FactIdentityDetectorReversedPeopleDict].([]string)
		}
	} else if exists {
		analyser.PeopleNumber = 0
	}

	return nil
}

func (analyser *CodeChurnAnalysis) ConfigureUpstream(_ map[string]interface{}) error {
	return nil
}

// Flag for the command line switch which enables this analysis.
func (analyser *CodeChurnAnalysis) Flag() string {
	return "codechurn"
}

// Description returns the text which explains what the analysis is doing.
func (analyser *CodeChurnAnalysis) Description() string {
	// TODO description
	return "Line burndown stats indicate the numbers of lines which were last edited within " +
		"specific time intervals through time. Search for \"git-of-theseus\" in the internet."
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (analyser *CodeChurnAnalysis) Initialize(repository *git.Repository) error {
	analyser.l = core.NewLogger()
	if analyser.Granularity <= 0 {
		analyser.l.Warnf("adjusted the granularity to %d ticks\n",
			DefaultBurndownGranularity)
		analyser.Granularity = DefaultBurndownGranularity
	}
	if analyser.Sampling <= 0 {
		analyser.l.Warnf("adjusted the sampling to %d ticks\n",
			DefaultBurndownGranularity)
		analyser.Sampling = DefaultBurndownGranularity
	}
	if analyser.Sampling > analyser.Granularity {
		analyser.l.Warnf("granularity may not be less than sampling, adjusted to %d\n",
			analyser.Granularity)
		analyser.Sampling = analyser.Granularity
	}
	analyser.repository = repository

	if analyser.PeopleNumber < 0 {
		return fmt.Errorf("PeopleNumber is negative: %d", analyser.PeopleNumber)
	}
	analyser.codeChurns = make([]personChurnStats, analyser.PeopleNumber)

	return nil
}

func (analyser *CodeChurnAnalysis) Fork(n int) []core.PipelineItem {
	return core.ForkSamePipelineItem(analyser, n)
}

// Consume runs this PipelineItem on the next commits data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (analyser *CodeChurnAnalysis) Consume(deps map[string]interface{}) (map[string]interface{}, error) {

	changes := deps[linehistory.DependencyLineHistory].(linehistory.LineHistoryChanges)
	analyser.fileResolver = changes.Resolver

	for _, change := range changes.Changes {
		if change.IsDelete() {
			continue
		}
		if change.PrevAuthor >= analyser.PeopleNumber && change.PrevAuthor != identity.AuthorMissing {
			change.PrevAuthor = identity.AuthorMissing
		}
		if change.CurrAuthor >= analyser.PeopleNumber && change.CurrAuthor != identity.AuthorMissing {
			change.CurrAuthor = identity.AuthorMissing
		}
		analyser.updateGlobal(change)

		if analyser.TrackFiles {
			analyser.updateFile(change)
		}

		analyser.updateAuthor(change)
		analyser.updateChurnMatrix(change)
	}

	return nil, nil
}

func (analyser *CodeChurnAnalysis) updateGlobal(change LineHistoryChange) {
}

// updateFile is bound to the specific `history` in the closure.
func (analyser *CodeChurnAnalysis) updateFile(change LineHistoryChange) {
}

func (analyser *CodeChurnAnalysis) updateAuthor(change LineHistoryChange) {
	if change.PrevAuthor == identity.AuthorMissing {
		return
	}
}

func (analyser *CodeChurnAnalysis) updateChurnMatrix(change LineHistoryChange) {
	if change.PrevAuthor == identity.AuthorMissing {
		return
	}
}

// Finalize returns the result of the analysis. Further calls to Consume() are not expected.
func (analyser *CodeChurnAnalysis) Finalize() interface{} {
	return nil
}

// Serialize converts the analysis result as returned by Finalize() to text or bytes.
// The text format is YAML and the bytes format is Protocol Buffers.
func (analyser *CodeChurnAnalysis) Serialize(result interface{}, binary bool, writer io.Writer) error {
	return nil
}

// Deserialize converts the specified protobuf bytes to BurndownResult.
func (analyser *CodeChurnAnalysis) Deserialize(message []byte) (interface{}, error) {
	return nil, nil
}

// MergeResults combines two BurndownResult-s together.
func (analyser *CodeChurnAnalysis) MergeResults(
	r1, r2 interface{}, c1, c2 *core.CommonAnalysisResult) interface{} {
	return nil
}

func init() {
	//	core.Registry.Register(&CodeChurnAnalysis{})
}
