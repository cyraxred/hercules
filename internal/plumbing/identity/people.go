package identity

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/cyraxred/hercules/internal/core"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/pkg/errors"
)

// PeopleDetector determines the author of a commit. Same person can commit under different
// signatures, and we apply some heuristics to merge those together.
// It is a PipelineItem.
type PeopleDetector struct {
	core.NoopMerger
	// PeopleDict maps email || name  -> developer id
	PeopleDict map[string]int
	// ReversedPeopleDict maps developer id -> description
	ReversedPeopleDict []string
	// ExactSignatures chooses the matching algorithm: opportunistic email || name
	// or exact email && name
	ExactSignatures bool
	Anonymity       bool

	l core.Logger
}

const (
	// FactIdentityDetectorReversedPeopleDict is the name of the fact which is inserted in
	// PeopleDetector.Configure(). It corresponds to PeopleDetector.ReversedPeopleDict -
	// the mapping from the author indices to the main signature.
	FactIdentityDetectorReversedPeopleDict = "IdentityDetector.ReversedPeopleDict"
	// ConfigIdentityDetectorPeopleDictPath is the name of the configuration option
	// (PeopleDetector.Configure()) which allows to set the external PeopleDict mapping from a file.
	ConfigIdentityDetectorPeopleDictPath = "PeopleDetector.PeopleDictPath"
	// ConfigIdentityDetectorExactSignatures is the name of the configuration option
	// (PeopleDetector.Configure()) which changes the matching algorithm to exact signature (name + email)
	// correspondence.
	ConfigIdentityDetectorExactSignatures = "PeopleDetector.ExactSignatures"

	ConfigIdentityDetectorAnonymity = "PeopleDetector.Anonymity"
)

var _ core.IdentityResolver = peopleResolver{}

type peopleResolver struct {
	identities *PeopleDetector
}

func (v peopleResolver) MaxCount() int {
	if v.identities == nil {
		return 0
	}
	return len(v.identities.ReversedPeopleDict)
}

func (v peopleResolver) Count() int {
	if v.identities == nil {
		return 0
	}
	return len(v.identities.ReversedPeopleDict)
}

func (v peopleResolver) nameOf(id core.AuthorId, anonymity bool) string {
	if id == core.AuthorMissing || id < 0 || v.identities == nil || int(id) >= len(v.identities.ReversedPeopleDict) {
		return core.AuthorMissingName
	}
	if !anonymity {
		return v.identities.ReversedPeopleDict[id]
	}
	return v.anonymizeName(id)
}

func (v peopleResolver) FriendlyNameOf(id core.AuthorId) string {
	return v.nameOf(id, v.identities.Anonymity)
}

func (v peopleResolver) PrivateNameOf(id core.AuthorId) string {
	return v.nameOf(id, false)
}

func (v peopleResolver) anonymizeName(id core.AuthorId) string {
	return fmt.Sprintf("Author %3d", id)
}

func (v peopleResolver) ForEachIdentity(callback func(core.AuthorId, string)) bool {
	if v.identities == nil {
		return false
	}
	for id, name := range v.identities.ReversedPeopleDict {
		if v.identities.Anonymity {
			name = v.anonymizeName(core.AuthorId(id))
		}
		callback(core.AuthorId(id), name)
	}
	return true
}

func (v peopleResolver) CopyNames(privateNames bool) []string {
	if v.identities == nil {
		return nil
	}
	if privateNames || !v.identities.Anonymity {
		return append([]string(nil), v.identities.ReversedPeopleDict...)
	}

	names := make([]string, len(v.identities.ReversedPeopleDict))
	for i := range names {
		names[i] = v.anonymizeName(core.AuthorId(i))
	}
	return names
}

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (detector *PeopleDetector) Name() string {
	return "PeopleDetector"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (detector *PeopleDetector) Provides() []string {
	return []string{DependencyAuthor}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (detector *PeopleDetector) Requires() []string {
	return []string{}
}

func (detector *PeopleDetector) Features() []string {
	return []string{core.FeatureGitCommits}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (detector *PeopleDetector) ListConfigurationOptions() []core.ConfigurationOption {
	return []core.ConfigurationOption{{
		Name:        ConfigIdentityDetectorPeopleDictPath,
		Description: "Path to the file with developer -> name|email associations.",
		Flag:        "people-dict",
		Type:        core.PathConfigurationOption,
		Default:     ""}, {
		Name: ConfigIdentityDetectorExactSignatures,
		Description: "Disable separate name/email matching. This will lead to considerbly more " +
			"identities and should not be normally used.",
		Flag:    "exact-signatures",
		Type:    core.BoolConfigurationOption,
		Default: false}, {
		Name:        ConfigIdentityDetectorAnonymity,
		Description: "Replaces identity info with sequential number.",
		Flag:        "people-anonymity",
		Type:        core.BoolConfigurationOption,
		Default:     false},
	}
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (detector *PeopleDetector) Configure(facts map[string]interface{}) error {
	if l, exists := facts[core.ConfigLogger].(core.Logger); exists {
		detector.l = l
	} else {
		detector.l = core.NewLogger()
	}

	detector.PeopleDict = nil
	if val, exists := facts[FactIdentityDetectorReversedPeopleDict].([]string); exists {
		detector.ReversedPeopleDict = val
	}

	if val, exists := facts[ConfigIdentityDetectorExactSignatures].(bool); exists {
		detector.ExactSignatures = val
	}

	if val, exists := facts[ConfigIdentityDetectorAnonymity].(bool); exists {
		detector.Anonymity = val
	}

	if peopleDictPath, ok := facts[ConfigIdentityDetectorPeopleDictPath].(string); ok && peopleDictPath != "" {
		err := detector.LoadPeopleDict(peopleDictPath)
		if err != nil {
			return errors.Errorf("failed to load %s: %v", peopleDictPath, err)
		}
	}

	if detector.ReversedPeopleDict == nil {
		if _, exists := facts[core.ConfigPipelineCommits]; !exists {
			panic("PeopleDetector needs a list of commits to initialize.")
		}
		detector.GeneratePeopleDict(facts[core.ConfigPipelineCommits].([]*object.Commit))
	}
	facts[FactIdentityDetectorReversedPeopleDict] = detector.ReversedPeopleDict

	if detector.PeopleDict == nil {
		detector.PeopleDict = make(map[string]int, len(detector.ReversedPeopleDict))
		for k, v := range detector.ReversedPeopleDict {
			detector.PeopleDict[v] = k
		}
	}

	var resolver core.IdentityResolver = peopleResolver{detector}
	facts[core.FactIdentityResolver] = resolver
	return nil
}

func (*PeopleDetector) ConfigureUpstream(map[string]interface{}) error {
	return nil
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (detector *PeopleDetector) Initialize(*git.Repository) error {
	detector.l = core.NewLogger()
	return nil
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (detector *PeopleDetector) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	commit := deps[core.DependencyCommit].(*object.Commit)
	var authorID int
	var exists bool
	signature := commit.Author
	if !detector.ExactSignatures {
		authorID, exists = detector.PeopleDict[strings.ToLower(signature.Email)]
		if !exists {
			authorID, exists = detector.PeopleDict[strings.ToLower(signature.Name)]
		}
	} else {
		authorID, exists = detector.PeopleDict[strings.ToLower(signature.String())]
	}
	if !exists {
		authorID = core.AuthorMissing
	}
	return map[string]interface{}{DependencyAuthor: authorID}, nil
}

// Fork clones this PipelineItem.
func (detector *PeopleDetector) Fork(n int) []core.PipelineItem {
	return core.ForkSamePipelineItem(detector, n)
}

// LoadPeopleDict loads author signatures from a text file.
// The format is one signature per line, and the signature consists of several
// keys separated by "|". The first key is the main one and used to reference all the rest.
func (detector *PeopleDetector) LoadPeopleDict(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	scanner := bufio.NewScanner(file)
	dict := make(map[string]int)
	var reverseDict []string
	size := 0
	for scanner.Scan() {
		ids := strings.Split(scanner.Text(), "|")
		canon := ids[0]
		var exists bool
		var canonIndex int
		// lookup or create a new canonical value
		if canonIndex, exists = dict[strings.ToLower(canon)]; !exists {
			reverseDict = append(reverseDict, canon)
			canonIndex = size
			size++
		}
		for _, id := range ids {
			dict[strings.ToLower(id)] = canonIndex
		}
	}
	detector.PeopleDict = dict
	detector.ReversedPeopleDict = reverseDict
	return nil
}

// GeneratePeopleDict loads author signatures from the specified list of Git commits.
func (detector *PeopleDetector) GeneratePeopleDict(commits []*object.Commit) {
	dict := map[string]int{}
	emails := map[int][]string{}
	names := map[int][]string{}
	size := 0

	mailmapFile, err := commits[len(commits)-1].File(".mailmap")
	// TODO(vmarkovtsev): properly handle .mailmap if ExactSignatures
	if !detector.ExactSignatures && err == nil {
		mailMapContents, err := mailmapFile.Contents()
		if err == nil {
			mailmap := ParseMailmap(mailMapContents)
			for key, val := range mailmap {
				key = strings.ToLower(key)
				toEmail := strings.ToLower(val.Email)
				toName := strings.ToLower(val.Name)
				id, exists := dict[toEmail]
				if !exists {
					id, exists = dict[toName]
				}
				if exists {
					dict[key] = id
				} else {
					id = size
					size++
					if toEmail != "" {
						dict[toEmail] = id
						emails[id] = append(emails[id], toEmail)
					}
					if toName != "" {
						dict[toName] = id
						names[id] = append(names[id], toName)
					}
					dict[key] = id
				}
				if strings.Contains(key, "@") {
					exists := false
					for _, val := range emails[id] {
						if key == val {
							exists = true
							break
						}
					}
					if !exists {
						emails[id] = append(emails[id], key)
					}
				} else {
					exists := false
					for _, val := range names[id] {
						if key == val {
							exists = true
							break
						}
					}
					if !exists {
						names[id] = append(names[id], key)
					}
				}
			}
		}
	}

	for _, commit := range commits {
		if !detector.ExactSignatures {
			email := strings.ToLower(commit.Author.Email)
			name := strings.ToLower(commit.Author.Name)
			id, exists := dict[email]
			if exists {
				_, exists := dict[name]
				if !exists {
					dict[name] = id
					names[id] = append(names[id], name)
				}
				continue
			}
			id, exists = dict[name]
			if exists {
				dict[email] = id
				emails[id] = append(emails[id], email)
				continue
			}
			dict[email] = size
			dict[name] = size
			emails[size] = append(emails[size], email)
			names[size] = append(names[size], name)
			size++
		} else { // !detector.ExactSignatures
			sig := strings.ToLower(commit.Author.String())
			if _, exists := dict[sig]; !exists {
				dict[sig] = size
				size++
			}
		}
	}
	reverseDict := make([]string, size)
	if !detector.ExactSignatures {
		for _, val := range dict {
			sort.Strings(names[val])
			sort.Strings(emails[val])
			reverseDict[val] = strings.Join(names[val], "|") + "|" + strings.Join(emails[val], "|")
		}
	} else {
		for key, val := range dict {
			reverseDict[val] = key
		}
	}
	detector.PeopleDict = dict
	detector.ReversedPeopleDict = reverseDict
}

func init() {
	core.Registry.RegisterPreferred(&PeopleDetector{}, true)
}
