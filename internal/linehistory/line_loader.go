package linehistory

import (
	"github.com/cyraxred/hercules/internal/core"
	items "github.com/cyraxred/hercules/internal/plumbing"
	"github.com/cyraxred/hercules/internal/plumbing/identity"
	"github.com/cyraxred/hercules/internal/rbtree"
	"github.com/go-git/go-git/v5"
)

// LineHistoryLoader allows to gather per-line history and statistics for a Git repository.
// It is a PipelineItem.
type LineHistoryLoader struct {
	fileNames map[FileId]string

	// files is the mapping <file path> -> *File.
	files map[string]*File

	// fileAllocator is the allocator for RBTree-s in `files`.
	fileAllocator *rbtree.Allocator

	l core.Logger
}

var _ core.FileIdResolver = loadedFileIdResolver{}

type loadedFileIdResolver struct {
	analyser *LineHistoryLoader
}

func (v loadedFileIdResolver) NameOf(id FileId) string {
	if v.analyser == nil {
		return ""
	}

	if n, ok := v.analyser.fileNames[id]; ok {
		return n
	}
	return v.abandonedNameOf(id)
}

func (v loadedFileIdResolver) abandonedNameOf(id FileId) string {
	return ""
}

func (v loadedFileIdResolver) MergedWith(id FileId) (FileId, string, bool) {
	if v.analyser == nil {
		return 0, "", false
	}

	switch f, n := v.analyser.findFileAndName(id); {
	case f != nil:
		return f.Id, n, true
	case n != "":
		return 0, n, false
	}
	return 0, v.abandonedNameOf(id), false
}

func (v loadedFileIdResolver) ForEachFile(callback func(id FileId, name string)) bool {
	if v.analyser == nil {
		return false
	}

	for name, file := range v.analyser.files {
		callback(file.Id, name)
	}
	return true
}

func (v loadedFileIdResolver) ScanFile(id FileId, callback func(line int, tick core.TickNumber, author core.AuthorId)) bool {
	if v.analyser == nil {
		return false
	}

	file, _ := v.analyser.findFileAndName(id)
	if file == nil {
		return false
	}
	file.ForEach(func(line, value int) {
		author, tick := unpackPersonWithTick(value)
		callback(line, tick, author)
	})
	return true
}

const (
	ConfigLinesLoadFrom = "LineHistory.LoadFrom"
)

func (analyser *LineHistoryLoader) Name() string {
	return "LineHistory"
}

func (analyser *LineHistoryLoader) Provides() []string {
	return []string{DependencyLineHistory, items.DependencyTick, identity.DependencyAuthor}
}

func (analyser *LineHistoryLoader) Requires() []string {
	return []string{}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (analyser *LineHistoryLoader) ListConfigurationOptions() []core.ConfigurationOption {
	return []core.ConfigurationOption{{
		Name:        ConfigLinesLoadFrom,
		Description: "Temporary directory where to save the hibernated RBTree allocators.",
		Flag:        "lines-load",
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

	var resolver core.FileIdResolver = loadedFileIdResolver{analyser}
	facts[core.FactLineHistoryResolver] = resolver

	return nil
}

func (analyser *LineHistoryLoader) ConfigureUpstream(_ map[string]interface{}) error {
	return nil
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (analyser *LineHistoryLoader) Initialize(repository *git.Repository) error {
	analyser.l = core.NewLogger()
	analyser.fileNames = map[FileId]string{}
	analyser.files = map[string]*File{}
	analyser.fileAllocator = rbtree.NewAllocator()
	//	analyser.fileAllocator.HibernationThreshold = analyser.HibernationThreshold

	return nil
}

func (analyser *LineHistoryLoader) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	if analyser.fileAllocator.Size() == 0 && len(analyser.files) > 0 {
		panic("LineHistoryLoader.Consume() was called on a hibernated instance")
	}

	//author := core.AuthorId(deps[identity.DependencyAuthor].(int))
	//analyser.tick = core.TickNumber(deps[items.DependencyTick].(int))

	result := map[string]interface{}{DependencyLineHistory: core.LineHistoryChanges{
		//Changes:  analyser.changes,
		//Resolver: FileIdResolver{analyser},
	}}

	return result, nil
}

func (analyser *LineHistoryLoader) findFileAndName(id FileId) (*File, string) {
	if n, ok := analyser.fileNames[id]; ok {
		if f := analyser.files[n]; f != nil {
			return f, n
		}
	}
	return nil, ""
}

// Fork clones this item. Everything is copied by reference except the files
// which are copied by value.
func (analyser *LineHistoryLoader) Fork(n int) []core.PipelineItem {
	result := make([]core.PipelineItem, n)

	for i := range result {
		clone := *analyser

		//clone.files = make(map[string]*File, len(analyser.files))
		//clone.fileNames = make(map[FileId]string, len(analyser.fileNames))
		//clone.fileAllocator = clone.fileAllocator.Clone()
		//for key, file := range analyser.files {
		//	clone.files[key] = file.CloneShallowWithUpdaters(clone.fileAllocator, clone.updateChangeList)
		//	clone.fileNames[file.Id] = key
		//}

		result[i] = &clone
	}

	return result
}

// Merge combines several items together. We apply the special file merging logic here.
func (analyser *LineHistoryLoader) Merge(items []core.PipelineItem) {
	analyser.onNewTick()

}

// Hibernate compresses the bound RBTree memory with the files.
func (analyser *LineHistoryLoader) Hibernate() error {
	analyser.fileAllocator.Hibernate()
	//if analyser.HibernationToDisk {
	//	file, err := ioutil.TempFile(analyser.HibernationDirectory, "*-hercules.bin")
	//	if err != nil {
	//		return err
	//	}
	//	analyser.hibernatedFileName = file.Name()
	//	err = file.Close()
	//	if err != nil {
	//		analyser.hibernatedFileName = ""
	//		return err
	//	}
	//	err = analyser.fileAllocator.Serialize(analyser.hibernatedFileName)
	//	if err != nil {
	//		analyser.hibernatedFileName = ""
	//		return err
	//	}
	//}
	return nil
}

// Boot decompresses the bound RBTree memory with the files.
func (analyser *LineHistoryLoader) Boot() error {
	//if analyser.hibernatedFileName != "" {
	//	err := analyser.fileAllocator.Deserialize(analyser.hibernatedFileName)
	//	if err != nil {
	//		return err
	//	}
	//	err = os.Remove(analyser.hibernatedFileName)
	//	if err != nil {
	//		return err
	//	}
	//	analyser.hibernatedFileName = ""
	//}
	//analyser.fileAllocator.Boot()
	return nil
}

func (analyser *LineHistoryLoader) onNewTick() {
}

//func (analyser *LineHistoryLoader) newFile(
//	_ plumbing.Hash, name string, author core.AuthorId, tick core.TickNumber, size int) (*File, error) {
//
//	fileId := analyser.fileIdCounter.next()
//	analyser.fileNames[fileId] = name
//	file := NewFile(fileId, packPersonWithTick(author, tick), size, analyser.fileAllocator, analyser.updateChangeList)
//	analyser.files[name] = file
//
//	return file, nil
//}

func (analyser *LineHistoryLoader) loadChangesFrom(name string) error {
	return nil
}

func init() {
	core.Registry.Register(&LineHistoryLoader{})
}
