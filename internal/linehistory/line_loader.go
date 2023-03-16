package linehistory

import (
	"bufio"
	"fmt"
	"github.com/cyraxred/hercules/internal/core"
	items "github.com/cyraxred/hercules/internal/plumbing"
	"github.com/cyraxred/hercules/internal/plumbing/identity"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"gopkg.in/yaml.v2"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// LineHistoryLoader allows to gather per-line history and statistics for a Git repository.
// It is a PipelineItem.
type LineHistoryLoader struct {
	files   map[FileId]fileInfo
	authors []string
	commits []commitInfo

	nextCommit int

	l core.Logger
}

type fileInfo struct {
	Name string
}

type commitInfo struct {
	Hash    plumbing.Hash
	Tick    core.TickNumber
	Author  core.AuthorId
	Changes []core.LineHistoryChange
}

func (v fileInfo) ForEach(func(line int, value int)) {
	panic("not implemented")
}

var _ core.FileIdResolver = loadedFileIdResolver{}

type loadedFileIdResolver struct {
	analyser *LineHistoryLoader
}

func (v loadedFileIdResolver) NameOf(id FileId) string {
	if v.analyser == nil {
		return ""
	}

	return v.analyser.files[id].Name
}

func (v loadedFileIdResolver) MergedWith(id FileId) (FileId, string, bool) {
	if v.analyser == nil {
		return 0, "", false
	}

	if f, ok := v.analyser.files[id]; ok {
		return id, f.Name, true
	}
	return 0, "", false
}

func (v loadedFileIdResolver) ForEachFile(callback func(id FileId, name string)) bool {
	if v.analyser == nil {
		return false
	}

	for id, file := range v.analyser.files {
		callback(id, file.Name)
	}
	return true
}

func (v loadedFileIdResolver) ScanFile(id FileId, callback func(line int, tick core.TickNumber, author core.AuthorId)) bool {
	if v.analyser == nil {
		return false
	}

	file, ok := v.analyser.files[id]
	if !ok {
		return false
	}
	file.ForEach(func(line, value int) {
		author, tick := unpackPersonWithTick(value)
		callback(line, tick, author)
	})
	return true
}

var _ core.IdentityResolver = authorResolver{}

type authorResolver struct {
	identities *LineHistoryLoader
}

func (v authorResolver) MaxCount() int {
	if v.identities == nil {
		return 0
	}
	return len(v.identities.authors)
}

func (v authorResolver) Count() int {
	if v.identities == nil {
		return 0
	}
	return len(v.identities.authors)
}

func (v authorResolver) PrivateNameOf(id core.AuthorId) string {
	return v.FriendlyNameOf(id)
}

func (v authorResolver) FriendlyNameOf(id core.AuthorId) string {
	if id == core.AuthorMissing || id < 0 || v.identities == nil || int(id) >= len(v.identities.authors) {
		return core.AuthorMissingName
	}
	return v.identities.authors[id]
}

func (v authorResolver) ForEachIdentity(callback func(core.AuthorId, string)) bool {
	if v.identities == nil {
		return false
	}
	for id, name := range v.identities.authors {
		callback(core.AuthorId(id), name)
	}
	return true
}

func (v authorResolver) CopyNames(bool) []string {
	if v.identities == nil {
		return nil
	}
	return append([]string(nil), v.identities.authors...)
}

const (
	ConfigLinesLoadFrom = "LineHistory.LoadFrom"
)

func (analyser *LineHistoryLoader) Name() string {
	return "LineHistoryLoader"
}

func (analyser *LineHistoryLoader) Provides() []string {
	return []string{DependencyLineHistory, items.DependencyTick, identity.DependencyAuthor}
}

func (analyser *LineHistoryLoader) Requires() []string {
	return []string{}
}

func (*LineHistoryLoader) Features() []string {
	return []string{core.FeatureGitStub}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (analyser *LineHistoryLoader) ListConfigurationOptions() []core.ConfigurationOption {
	return []core.ConfigurationOption{{
		Name:        ConfigLinesLoadFrom,
		Description: "Temporary directory where to save the hibernated RBTree allocators.",
		Flag:        "history-line-load",
		Type:        core.PathConfigurationOption,
		Default:     ""},
	}
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (analyser *LineHistoryLoader) Configure(facts map[string]interface{}) error {
	if l, exists := facts[core.ConfigLogger].(core.Logger); exists {
		analyser.l = l
	} else {
		analyser.l = core.NewLogger()
	}
	if val, exists := facts[ConfigLinesLoadFrom].(string); exists {
		if err := analyser.loadChangesFrom(val); err != nil {
			return err
		}
	}

	facts[core.FactLineHistoryResolver] = loadedFileIdResolver{analyser}
	facts[core.FactIdentityResolver] = authorResolver{analyser}

	facts[core.ConfigPipelineCommits] = analyser.buildCommits()

	return nil
}

func (analyser *LineHistoryLoader) ConfigureUpstream(_ map[string]interface{}) error {
	return nil
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (analyser *LineHistoryLoader) Initialize(*git.Repository) error {
	analyser.l = core.NewLogger()
	analyser.files = map[FileId]fileInfo{}
	analyser.nextCommit = 0

	return nil
}

func (analyser *LineHistoryLoader) Consume(map[string]interface{}) (map[string]interface{}, error) {

	var commit commitInfo
	if analyser.nextCommit < len(analyser.commits) {
		commit = analyser.commits[analyser.nextCommit]
		analyser.nextCommit++
	} else {
		commit.Author = core.AuthorMissing
	}

	result := map[string]interface{}{
		DependencyLineHistory: core.LineHistoryChanges{
			Changes:  commit.Changes,
			Resolver: loadedFileIdResolver{analyser},
		},
		items.DependencyTick:      int(commit.Tick),
		identity.DependencyAuthor: int(commit.Author),
	}

	return result, nil
}

func (analyser *LineHistoryLoader) Fork(n int) []core.PipelineItem {
	return core.ForkSamePipelineItem(analyser, n)
}

func (analyser *LineHistoryLoader) Merge([]core.PipelineItem) {
	analyser.l.Critical("cant be merged")
}

func (analyser *LineHistoryLoader) loadChangesFrom(name string) error {
	if input, err := os.Open(name); err == nil {
		defer func() { _ = input.Close() }()
		return analyser.loadChangesFromYaml(yaml.NewDecoder(input))
	} else {
		return err
	}
}

var regexSplitBySpace = regexp.MustCompile("\\s+")

func (analyser *LineHistoryLoader) loadChangesFromYaml(decoder *yaml.Decoder) error {
	type dumperScheme struct {
		Commits yaml.MapSlice     `yaml:"commits"`
		Authors []string          `yaml:"author_sequence"`
		Files   map[FileId]string `yaml:"file_sequence"`
	}
	values := struct {
		LineDumper dumperScheme `yaml:"LineDumper"`
	}{}

	if err := decoder.Decode(&values); err != nil {
		return err
	}

	analyser.authors = values.LineDumper.Authors

	analyser.files = make(map[FileId]fileInfo, len(values.LineDumper.Files))
	for k, v := range values.LineDumper.Files {
		analyser.files[k] = fileInfo{Name: v}
	}

	analyser.commits = make([]commitInfo, 0, len(values.LineDumper.Commits))
	for _, yamlCommit := range values.LineDumper.Commits {
		analyser.commits = append(analyser.commits, commitInfo{})
		info := &analyser.commits[len(analyser.commits)-1]
		for r := bufio.NewScanner(strings.NewReader(yamlCommit.Value.(string))); r.Scan(); {
			line := r.Text()
			chunks := regexSplitBySpace.Split(line, -1)
			if len(chunks) != 6 {
				return fmt.Errorf("unexpected number of fields '%d' from: %s", len(chunks), line)
			}
			vals := make([]int, len(chunks))
			for i, s := range chunks {
				v, err := strconv.Atoi(s)
				if err != nil {
					return fmt.Errorf("unable to parse '%s' from: %s", s, line)
				}
				vals[i] = v
			}
			change := core.LineHistoryChange{
				FileId:     core.FileId(vals[0]),
				PrevAuthor: core.AuthorId(vals[1]),
				PrevTick:   core.TickNumber(vals[2]),
				CurrAuthor: core.AuthorId(vals[3]),
				CurrTick:   core.TickNumber(vals[4]),
				Delta:      vals[5],
			}
			info.Changes = append(info.Changes, change)
		}
		info.Tick = info.Changes[0].CurrTick
		info.Author = info.Changes[0].CurrAuthor
		info.Hash = plumbing.NewHash(yamlCommit.Key.(string))
	}

	return nil
}

func (analyser *LineHistoryLoader) buildCommits() (result []*object.Commit) {
	result = make([]*object.Commit, 0, len(analyser.commits))

	var parentHash []plumbing.Hash
	for _, commit := range analyser.commits {
		result = append(result, &object.Commit{
			Hash:         commit.Hash,
			Author:       object.Signature{},
			Committer:    object.Signature{},
			PGPSignature: "",
			Message:      "",
			TreeHash:     plumbing.Hash{},
			ParentHashes: parentHash,
		})
		parentHash = []plumbing.Hash{commit.Hash}
	}

	return
}

func init() {
	core.Registry.Register(&LineHistoryLoader{})
}
