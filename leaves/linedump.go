package leaves

import (
	"fmt"
	"github.com/cyraxred/hercules/internal/core"
	"github.com/cyraxred/hercules/internal/linehistory"
	"github.com/cyraxred/hercules/internal/plumbing/identity"
	"github.com/cyraxred/hercules/internal/yaml"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"io"
	"os"
	"sort"
)

type LineDumperCommit struct {
	CommitHash plumbing.Hash
	Changes    []core.LineHistoryChange
}

type LineDumperResult struct {
	Commits            []LineDumperCommit
	fileDict           map[core.FileId]string
	reversedAuthorDict []string
}

type LineDumper struct {
	core.NoopMerger

	AuthorDictOut string

	peopleResolver  core.IdentityResolver
	primaryResolver core.FileIdResolver

	commits []LineDumperCommit

	l core.Logger
}

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (analyser *LineDumper) Name() string {
	return "LineDumper"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (analyser *LineDumper) Provides() []string {
	return []string{}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (analyser *LineDumper) Requires() []string {
	return []string{linehistory.DependencyLineHistory, identity.DependencyAuthor}
}

const ConfigLineDumperAuthorDict = "LineDumper.AuthorDict"

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (analyser *LineDumper) ListConfigurationOptions() []core.ConfigurationOption {
	return []core.ConfigurationOption{{
		Name:        ConfigLineDumperAuthorDict,
		Description: "Path to the output file with authors.",
		Flag:        "author-dict-out",
		Type:        core.PathConfigurationOption,
		Default:     ""},
	}
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (analyser *LineDumper) Configure(facts map[string]interface{}) error {
	if l, exists := facts[core.ConfigLogger].(core.Logger); exists {
		analyser.l = l
	} else {
		analyser.l = core.NewLogger()
	}

	if val, exists := facts[ConfigLineDumperAuthorDict].(string); exists {
		analyser.AuthorDictOut = val
	}

	analyser.peopleResolver, _ = facts[core.FactIdentityResolver].(core.IdentityResolver)

	if resolver, exists := facts[core.FactLineHistoryResolver].(core.FileIdResolver); exists {
		analyser.primaryResolver = resolver
	}

	return nil
}

func (analyser *LineDumper) ConfigureUpstream(map[string]interface{}) error {
	return nil
}

// Flag for the command line switch which enables this analysis.
func (analyser *LineDumper) Flag() string {
	return "linedump"
}

// Description returns the text which explains what the analysis is doing.
func (analyser *LineDumper) Description() string {
	return "Dumps raw history of line changes by authors"
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (analyser *LineDumper) Initialize(*git.Repository) error {
	analyser.l = core.NewLogger()

	if analyser.peopleResolver == nil {
		analyser.peopleResolver = core.NewIdentityResolver(nil, nil)
	}
	analyser.commits = nil

	return nil
}

func (analyser *LineDumper) Fork(n int) []core.PipelineItem {
	return core.ForkSamePipelineItem(analyser, n)
}

// Consume runs this PipelineItem on the next commits data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (analyser *LineDumper) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	commit := deps[core.DependencyCommit].(*object.Commit)

	changes := deps[linehistory.DependencyLineHistory].(core.LineHistoryChanges)
	if analyser.primaryResolver == nil {
		analyser.primaryResolver = changes.Resolver
	}

	if len(changes.Changes) == 0 {
		return nil, nil
	}

	analyser.commits = append(analyser.commits, LineDumperCommit{
		CommitHash: commit.Hash,
		Changes:    append([]core.LineHistoryChange(nil), changes.Changes...),
	})

	return nil, nil
}

// Finalize returns the result of the analysis. Further calls to Consume() are not expected.
func (analyser *LineDumper) Finalize() interface{} {
	if analyser.AuthorDictOut != "" {
		names := analyser.peopleResolver.CopyNames(true)
		if err := writeLines(names, analyser.AuthorDictOut); err != nil {
			_, _ = fmt.Fprint(os.Stderr, "unable to write to the file:", analyser.AuthorDictOut, err)
		}
	}

	fileNames := map[core.FileId]string{}
	analyser.primaryResolver.ForEachFile(func(id core.FileId, name string) {
		fileNames[id] = name
	})

	return LineDumperResult{
		Commits:            analyser.commits,
		fileDict:           fileNames,
		reversedAuthorDict: analyser.peopleResolver.CopyNames(false),
	}
}

func writeLines(lines []string, out string) error {
	authors, err := os.Create(out)
	if err != nil {
		return err
	}

	defer func() {
		_ = authors.Close()
	}()
	for _, name := range lines {
		if _, err = authors.WriteString(name + "\n"); err != nil {
			return err
		}
	}

	return nil
}

// Serialize converts the analysis result as returned by Finalize() to text or bytes.
// The text format is YAML and the bytes format is Protocol Buffers.
func (analyser *LineDumper) Serialize(result interface{}, binary bool, writer io.Writer) error {
	typedResult, ok := result.(LineDumperResult)
	if !ok {
		return fmt.Errorf("result is not a line dumper result: '%v'", result)
	}
	if binary {
		return fmt.Errorf("binary is not supported")
	}
	analyser.serializeText(typedResult, writer)
	return nil
}

func (analyser *LineDumper) serializeText(result LineDumperResult, writer io.Writer) {
	if len(result.Commits) == 0 {
		return
	}

	_, _ = fmt.Fprintln(writer, "  commits: ")
	for _, commit := range result.Commits {
		hash := commit.CommitHash.String()
		_, _ = fmt.Fprintf(writer, "    %s: |-\n", hash)
		for _, change := range commit.Changes {
			_, _ = fmt.Fprintf(writer, "      %d %6d %5d %6d %5d %d\n", change.FileId,
				change.PrevAuthor, change.PrevTick,
				change.CurrAuthor, change.CurrTick, change.Delta)
		}
	}

	if len(result.fileDict) > 0 {
		ids := make([]int, 0, len(result.fileDict))
		for k := range result.fileDict {
			ids = append(ids, int(k))
		}
		sort.Ints(ids)

		_, _ = fmt.Fprintln(writer, "  file_sequence:")
		for _, k := range ids {
			v := result.fileDict[core.FileId(k)]
			_, _ = fmt.Fprintf(writer, "    %d: %s\n", k, yaml.SafeString(v))
		}
	}

	if len(result.reversedAuthorDict) > 0 {
		_, _ = fmt.Fprintln(writer, "  author_sequence:")
		for _, v := range result.reversedAuthorDict {
			_, _ = fmt.Fprintln(writer, "    - "+yaml.SafeString(v))
		}
	}
}

func init() {
	core.Registry.RegisterPreferred(&LineDumper{}, false)
}
