package main

import (
	"bytes"
	"fmt"
	"github.com/cyraxred/hercules/internal/core"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"path/filepath"
	"plugin"
	"regexp"
	"runtime/pprof"
	"sort"
	"strings"
	"text/template"
	"unicode"

	"github.com/Masterminds/sprig"
	"github.com/cyraxred/go-billy-siva"
	"github.com/cyraxred/hercules"
	"github.com/cyraxred/hercules/internal/pb"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/transport/ssh"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/go-git/go-git/v5/storage/memory"
	"github.com/gogo/protobuf/proto"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"golang.org/x/crypto/ssh/terminal"
	progress "gopkg.in/cheggaaa/pb.v1"
)

// oneLineWriter splits the output data by lines and outputs one on top of another using '\r'.
// It also does some dark magic to handle Git statuses.
type oneLineWriter struct {
	Writer io.Writer
}

func (writer oneLineWriter) Write(p []byte) (n int, err error) {
	strp := strings.TrimSpace(string(p))
	if strings.HasSuffix(strp, "done.") || len(strp) == 0 {
		strp = "cloning..."
	} else {
		strp = strings.Replace(strp, "\n", "\033[2K\r", -1)
	}
	_, err = writer.Writer.Write([]byte("\033[2K\r"))
	if err != nil {
		return
	}
	n, err = writer.Writer.Write([]byte(strp))
	return
}

func loadSSHIdentity(sshIdentity string) (*ssh.PublicKeys, error) {
	actual, err := homedir.Expand(sshIdentity)
	if err != nil {
		return nil, err
	}
	return ssh.NewPublicKeysFromFile("git", actual, "")
}

var regexUri = regexp.MustCompile("^[A-Za-z]\\w*@[A-Za-z0-9][\\w.]*:")

func loadRepository(uri string, cachePath string, disableStatus bool, sshIdentity string,
) (repository *git.Repository, repoUri string, repoFeature string) {
	var err error
	repository, repoUri, repoFeature, err = loadRepositoryWithError(uri, cachePath, disableStatus, sshIdentity)
	if err != nil {
		log.Panicf("failed to open %s: %v", uri, err)
	}
	return
}

func loadRepositoryWithError(uri string, cachePath string, disableStatus bool, sshIdentity string,
) (repository *git.Repository, repoUri string, repoFeature string, err error) {
	repoFeature = core.FeatureGitCommits
	repoUri = uri

	if uri == "-" && cachePath == "" {
		repository, err = git.Init(memory.NewStorage(), memfs.New())
		w, _ := repository.Worktree()
		if _, err = w.Commit("Initial", &git.CommitOptions{AllowEmptyCommits: true}); err != nil {
			log.Panicf("failed to create a virtual repo: %v", err)
		}
		repoFeature = core.FeatureGitStub
	} else if strings.Contains(uri, "://") || regexUri.MatchString(uri) {
		var backend storage.Storer
		if cachePath != "" {
			backend = filesystem.NewStorage(osfs.New(cachePath), cache.NewObjectLRUDefault())
			_, err = os.Stat(cachePath)
			if !os.IsNotExist(err) {
				log.Printf("warning: deleted %s\n", cachePath)
				_ = os.RemoveAll(cachePath)
			}
		} else {
			backend = memory.NewStorage()
		}

		cloneOptions := &git.CloneOptions{URL: uri}

		if parsed, err2 := url.Parse(uri); err2 == nil {
			if parsed.User != nil {
				parsed.User = nil
				repoUri = parsed.String()
			}
		}

		if !disableStatus {
			_, _ = fmt.Fprint(os.Stderr, "connecting...\r")
			cloneOptions.Progress = oneLineWriter{Writer: os.Stderr}
		}

		if sshIdentity != "" {
			auth, err2 := loadSSHIdentity(sshIdentity)
			if err2 != nil {
				log.Printf("Failed loading SSH Identity %s\n", err)
			}
			cloneOptions.Auth = auth
		}

		repository, err = git.Clone(backend, nil, cloneOptions)
		if !disableStatus {
			_, _ = fmt.Fprint(os.Stderr, "\033[2K\r")
		}

	} else if stat, err2 := os.Stat(uri); err2 == nil && !stat.IsDir() {
		localFs := osfs.New(filepath.Dir(uri))
		tmpFs := memfs.New()
		basePath := filepath.Base(uri)
		fs, err3 := sivafs.NewFilesystem(localFs, basePath, tmpFs)
		if err3 != nil {
			log.Panicf("unable to create a siva filesystem from %s: %v", uri, err2)
		}
		sivaStorage := filesystem.NewStorage(fs, cache.NewObjectLRUDefault())
		repository, err = git.Open(sivaStorage, tmpFs)
	} else {
		if uri[len(uri)-1] == os.PathSeparator {
			uri = uri[:len(uri)-1]
		}
		repository, err = git.PlainOpen(uri)
	}

	return
}

type arrayPluginFlags map[string]bool

func (apf *arrayPluginFlags) String() string {
	var list []string
	for key := range *apf {
		list = append(list, key)
	}
	return strings.Join(list, ", ")
}

func (apf *arrayPluginFlags) Set(value string) error {
	(*apf)[value] = true
	return nil
}

func (apf *arrayPluginFlags) Type() string {
	return "path"
}

func loadPlugins() {
	pluginFlags := arrayPluginFlags{}
	fs := pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	pluginFlagName := "plugin"
	const pluginDesc = "Load the specified plugin by the full or relative path. " +
		"Can be specified multiple times."
	fs.Var(&pluginFlags, pluginFlagName, pluginDesc)
	err := cobra.MarkFlagFilename(fs, "plugin")
	if err != nil {
		panic(err)
	}
	pflag.Var(&pluginFlags, pluginFlagName, pluginDesc)
	_ = fs.Parse(os.Args[1:])
	for path := range pluginFlags {
		_, err := plugin.Open(path)
		if err != nil {
			log.Printf("Failed to load plugin from %s %s\n", path, err)
		}
	}
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "hercules",
	Short: "Analyse a Git repository.",
	Long: `Hercules is a flexible and fast Git repository analysis engine. The base command executes
the commit processing pipeline which is automatically generated from the dependencies of one
or several analysis targets. The list of the available targets is printed in --help. External
targets can be added using the --plugin system.`,
	Args: cobra.RangeArgs(1, 2),
	Run: func(cmd *cobra.Command, args []string) {
		flags := cmd.Flags()
		getBool := func(name string) bool {
			value, err := flags.GetBool(name)
			if err != nil {
				panic(err)
			}
			return value
		}
		getString := func(name string) string {
			value, err := flags.GetString(name)
			if err != nil {
				panic(err)
			}
			return value
		}
		firstParent := getBool("first-parent")
		commitsFile := getString("commits")
		head := getBool("head")
		protobuf := getBool("pb")
		profile := getBool("profile")
		disableStatus := getBool("quiet")
		sshIdentity := getString("ssh-identity")

		if profile {
			go func() {
				err := http.ListenAndServe("localhost:6060", nil)
				if err != nil {
					panic(err)
				}
			}()
			prof, _ := os.Create("hercules.pprof")
			err := pprof.StartCPUProfile(prof)
			if err != nil {
				panic(err)
			}
			defer pprof.StopCPUProfile()
		}
		uri := args[0]
		cachePath := ""
		if len(args) == 2 {
			cachePath = args[1]
		}
		repository, repoUri, repoFeature := loadRepository(uri, cachePath, disableStatus, sshIdentity)

		// core logic
		pipeline := hercules.NewPipeline(repository)
		if repoFeature != "" {
			pipeline.SetFeature(repoFeature)
		}
		pipeline.SetFeaturesFromFlags()
		var bar *progress.ProgressBar
		if !disableStatus {
			pipeline.OnProgress = func(commit, length int, action string) {
				if bar == nil {
					bar = progress.New(length)
					bar.Callback = func(msg string) {
						_, _ = os.Stderr.WriteString("\033[2K\r" + msg)
					}
					bar.NotPrint = true
					bar.ShowPercent = false
					bar.ShowSpeed = false
					bar.SetMaxWidth(80).Start()
				}
				if action == hercules.MessageFinalize {
					bar.Finish()
					_, _ = fmt.Fprint(os.Stderr, "\033[2K\rfinalizing...")
				} else {
					bar.Set(commit).Postfix(" [" + action + "] ")
				}
			}
		}

		if repoFeature == core.FeatureGitCommits {
			var commits []*object.Commit
			var err error
			if commitsFile == "" {
				if !head {
					_, _ = fmt.Fprint(os.Stderr, "git log...\r")
					commits, err = pipeline.Commits(firstParent)
				} else {
					commits, err = pipeline.HeadCommit()
				}
			} else {
				commits, err = hercules.LoadCommitsFromFile(commitsFile, repository)
			}
			if err != nil {
				log.Fatalf("failed to list the commits: %v", err)
			}
			cmdlineFacts[hercules.ConfigPipelineCommits] = commits
		}

		var priorityFn = func(items []core.PipelineItem) core.PipelineItem {
			if len(items) == 0 {
				return nil
			}
			if len(items) > 1 {
				sort.Stable(&flagSorter{items: items, flagSet: flags, featureSet: pipeline})
			}
			return items[0]
		}

		pipeline.DryRun, _ = cmdlineFacts[hercules.ConfigPipelineDryRun].(bool)
		deployedLeafs := deployItemsToPipeline(pipeline, flags, priorityFn)

		if err := pipeline.InitializeExt(cmdlineFacts, priorityFn, true); err != nil {
			log.Fatal(err)
		}

		results, err := pipeline.RunPreparedPlan()
		if err != nil {
			log.Fatalf("failed to run the pipeline: %v", err)
		}
		if !disableStatus {
			_, _ = fmt.Fprint(os.Stderr, "\033[2K\r")
			// if not a terminal, the user will not see the output, so show the status
			if !terminal.IsTerminal(int(os.Stdout.Fd())) {
				_, _ = fmt.Fprint(os.Stderr, "writing...\r")
			}
		}
		if protobuf {
			protobufResults(repoUri, deployedLeafs, results)
		} else {
			printResults(repoUri, deployedLeafs, results)
		}
	},
}

func deployItemsToPipeline(pipeline *core.Pipeline, flags *pflag.FlagSet,
	priorityFn func(items []core.PipelineItem) core.PipelineItem,
) (deployed []hercules.LeafPipelineItem) {
	deployList := make([][]string, 0, len(cmdlineDeployed))
	for name, valPtr := range cmdlineDeployed {
		if *valPtr {
			deployList = append(deployList, []string{name})
		}
	}

	flags.Visit(func(flag *pflag.Flag) {
		if names := activationByFlags[flag.Name]; len(names) > 0 {
			deployList = append(deployList, names)
		}
	})

	for _, names := range deployList {
		switch summons := hercules.Registry.Summon(names...); {
		case len(summons) == 0:
			log.Fatalf("missing item(s): %v", names)
		case len(summons) > 1:
			if len(names) == 2 {
				log.Printf("ambigous item: %v", names)
			}
			summons[0] = priorityFn(summons)
			summons = summons[:1]
			fallthrough
		default:
			item := pipeline.DeployItemOnce(summons[0])
			if !pipeline.DryRun && item == summons[0] {
				deployed = append(deployed, item.(hercules.LeafPipelineItem))
			}
		}
	}

	return
}

type flagSorter struct {
	items      []core.PipelineItem
	flagSet    *pflag.FlagSet
	featureSet interface {
		GetFeature(string) (bool, bool)
	}
	cache []int
}

func (v *flagSorter) Len() int {
	return len(v.items)
}

func (v *flagSorter) Less(i, j int) bool {
	if v.cache == nil {
		v.cache = make([]int, len(v.items))
	}
	return v.itemWeight(i) > v.itemWeight(j)
}

func (v *flagSorter) Swap(i, j int) {
	v.cache[i], v.cache[j] = v.cache[j], v.cache[i]
	v.items[i], v.items[j] = v.items[j], v.items[i]
}

func (v *flagSorter) itemWeight(i int) int {
	if w := v.cache[i]; w != 0 {
		return w
	}
	w := v.weightFlagsOf(v.items[i], v.flagSet)
	v.cache[i] = w + 1
	return w
}

func (v *flagSorter) weightFlagsOf(item core.PipelineItem, flagSet *pflag.FlagSet) int {
	const (
		weightProvide   = -1 // excessive provides are not welcome
		weightParamFlag = 100
		weightFeature   = 100
	)

	w := weightProvide * len(item.Provides())
	for _, opt := range item.ListConfigurationOptions() {
		if flagSet.Changed(opt.Flag) {
			w += weightParamFlag
		}
	}
	if featured, ok := item.(core.FeaturedPipelineItem); v.featureSet != nil && ok {
		for _, feat := range featured.Features() {
			if ok, _ := v.featureSet.GetFeature(feat); ok {
				w += weightFeature
			}
		}
	}
	return w
}

func printResults(
	uri string, deployed []hercules.LeafPipelineItem,
	results map[hercules.LeafPipelineItem]interface{}) {
	commonResult := results[nil].(*hercules.CommonAnalysisResult)

	fmt.Println("hercules:")
	fmt.Printf("  version: %d\n", hercules.BinaryVersion)
	fmt.Println("  hash:", hercules.BinaryGitHash)
	fmt.Println("  repository:", uri)
	fmt.Println("  begin_unix_time:", commonResult.BeginTime)
	fmt.Println("  end_unix_time:", commonResult.EndTime)
	fmt.Println("  commits:", commonResult.CommitsNumber)
	fmt.Println("  run_time:", commonResult.RunTime.Nanoseconds()/1e6)

	for _, item := range deployed {
		result := results[item]
		fmt.Printf("%s:\n", item.Name())
		if err := item.Serialize(result, false, os.Stdout); err != nil {
			panic(err)
		}
	}
}

func protobufResults(
	uri string, deployed []hercules.LeafPipelineItem,
	results map[hercules.LeafPipelineItem]interface{}) {

	header := pb.Metadata{
		Version:    2,
		Hash:       hercules.BinaryGitHash,
		Repository: uri,
	}
	results[nil].(*hercules.CommonAnalysisResult).FillMetadata(&header)

	message := pb.AnalysisResults{
		Header:   &header,
		Contents: map[string][]byte{},
	}

	for _, item := range deployed {
		result := results[item]
		buffer := &bytes.Buffer{}
		if err := item.Serialize(result, true, buffer); err != nil {
			panic(err)
		}
		message.Contents[item.Name()] = buffer.Bytes()
	}

	serialized, err := proto.Marshal(&message)
	if err != nil {
		panic(err)
	}
	_, _ = os.Stdout.Write(serialized)
}

// trimRightSpace removes the trailing whitespace characters.
func trimRightSpace(s string) string {
	return strings.TrimRightFunc(s, unicode.IsSpace)
}

// rpad adds padding to the right of a string.
func rpad(s string, padding int) string {
	return fmt.Sprintf(fmt.Sprintf("%%-%ds", padding), s)
}

// tmpl was adapted from cobra/cobra.go
func tmpl(w io.Writer, text string, data interface{}) error {
	var templateFuncs = template.FuncMap{
		"trim":                    strings.TrimSpace,
		"trimRightSpace":          trimRightSpace,
		"trimTrailingWhitespaces": trimRightSpace,
		"rpad":                    rpad,
		"gt":                      cobra.Gt,
		"eq":                      cobra.Eq,
	}
	for k, v := range sprig.TxtFuncMap() {
		templateFuncs[k] = v
	}
	t := template.New("top")
	t.Funcs(templateFuncs)
	template.Must(t.Parse(text))
	return t.Execute(w, data)
}

func formatUsage(c *cobra.Command) error {
	// the default UsageFunc() does some private magic c.mergePersistentFlags()
	// this should stay on top
	localFlags := c.LocalFlags()
	leaves := hercules.Registry.GetLeaves()
	plumbing := hercules.Registry.GetPlumbingItems()
	features := hercules.Registry.GetFeaturedItems()
	hercules.EnablePathFlagTypeMasquerade()
	filter := map[string]bool{}
	for _, l := range leaves {
		filter[l.Flag()] = true
		for _, cfg := range l.ListConfigurationOptions() {
			filter[cfg.Flag] = true
		}
	}
	for _, i := range plumbing {
		for _, cfg := range i.ListConfigurationOptions() {
			filter[cfg.Flag] = true
		}
	}

	for key := range filter {
		localFlags.Lookup(key).Hidden = true
	}
	args := map[string]interface{}{
		"c":        c,
		"leaves":   leaves,
		"plumbing": plumbing,
		"features": features,
	}

	helpTemplate := `Usage:{{if .c.Runnable}}
  {{.c.UseLine}}{{end}}{{if .c.HasAvailableSubCommands}}
  {{.c.CommandPath}} [command]{{end}}{{if gt (len .c.Aliases) 0}}

Aliases:
  {{.c.NameAndAliases}}{{end}}{{if .c.HasExample}}

Examples:
{{.c.Example}}{{end}}{{if .c.HasAvailableSubCommands}}

Available Commands:{{range .c.Commands}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{if .c.HasAvailableLocalFlags}}

Flags:
{{range $line := .c.LocalFlags.FlagUsages | trimTrailingWhitespaces | split "\n"}}
{{- $desc := splitList "   " $line | last}}
{{- $offset := sub ($desc | len) ($desc | trim | len)}}
{{- $indent := splitList "   " $line | initial | join "   " | len | add 3 | add $offset | int}}
{{- $wrap := sub 120 $indent | int}}
{{- splitList "   " $line | initial | join "   "}}   {{cat "!" $desc | wrap $wrap | indent $indent | substr $indent -1 | substr 2 -1}}
{{end}}{{end}}

Analysis Targets:{{range .leaves}}
      --{{rpad .Flag 40}}Runs {{.Name}} analysis.{{wrap 72 .Description | nindent 48}}{{range .ListConfigurationOptions}}
          --{{if .Type.String}}{{rpad (print .Flag " " .Type.String) 40}}{{else}}{{rpad .Flag 40}}{{end}}
          {{- $desc := dict "desc" .Description}}
          {{- if .Default}}{{$_ := set $desc "desc" (print .Description " The default value is " .FormatDefault ".")}}
          {{- end}}
          {{- $desc := pluck "desc" $desc | first}}
          {{- $desc | wrap 68 | indent 52 | substr 52 -1}}{{end}}
{{end}}

Plumbing Options:{{range .plumbing}}{{$name := .Name}}{{range .ListConfigurationOptions}}
      --{{if .Type.String}}{{rpad (print .Flag " " .Type.String " [" $name "]") 40}}{{else}}{{rpad (print .Flag " [" $name "]") 40}}
        {{- end}}
        {{- $desc := dict "desc" .Description}}
        {{- if .Default}}{{$_ := set $desc "desc" (print .Description " The default value is " .FormatDefault ".")}}
        {{- end}}
        {{- $desc := pluck "desc" $desc | first}}{{$desc | wrap 72 | indent 48 | substr 48 -1}}{{end}}{{end}}

--feature:{{range $key, $value := .features}}
      {{rpad $key 42}}Enables {{range $index, $item := $value}}{{if $index}}, {{end}}{{$item.Name}}{{end}}.{{end}}{{if .c.HasAvailableInheritedFlags}}

Global Flags:
{{.c.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .c.HasHelpSubCommands}}

Additional help topics:{{range .c.Commands}}{{if .IsAdditionalHelpTopicCommand}}
  {{rpad .CommandPath .CommandPathPadding}} {{.Short}}{{end}}{{end}}{{end}}{{if .c.HasAvailableSubCommands}}

Use "{{.c.CommandPath}} [command] --help" for more information about a command.{{end}}
`
	err := tmpl(c.OutOrStderr(), helpTemplate, args)
	for key := range filter {
		localFlags.Lookup(key).Hidden = false
	}
	if err != nil {
		c.Println(err)
	}
	return err
}

// versionCmd prints the API version and the Git commit hash
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information and exit.",
	Long:  ``,
	Args:  cobra.MaximumNArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("Version: %d\nGit:     %s\n", hercules.BinaryVersion, hercules.BinaryGitHash)
	},
}

var cmdlineFacts map[string]interface{}
var cmdlineDeployed map[string]*bool
var activationByFlags map[string][]string

func init() {
	loadPlugins()
	rootFlags := rootCmd.Flags()
	rootFlags.String("commits", "", "Path to the text file with the "+
		"commit history to follow instead of the default 'git log'. "+
		"The format is the list of hashes, each hash on a "+
		"separate line. The first hash is the root.")
	err := rootCmd.MarkFlagFilename("commits")
	if err != nil {
		panic(err)
	}
	hercules.PathifyFlagValue(rootFlags.Lookup("commits"))
	rootFlags.Bool("head", false, "Analyze only the latest commit.")
	rootFlags.Bool("first-parent", false, "Follow only the first parent in the commit history - "+
		"\"git log --first-parent\".")
	rootFlags.Bool("pb", false, "The output format will be Protocol Buffers instead of YAML.")
	rootFlags.Bool("quiet", !terminal.IsTerminal(int(os.Stdin.Fd())),
		"Do not print status updates to stderr.")
	rootFlags.Bool("profile", false, "Collect the profile to hercules.pprof.")
	rootFlags.String("ssh-identity", "", "Path to SSH identity file (e.g., ~/.ssh/id_rsa) to clone from an SSH remote.")
	err = rootCmd.MarkFlagFilename("ssh-identity")
	if err != nil {
		panic(err)
	}
	hercules.PathifyFlagValue(rootFlags.Lookup("ssh-identity"))
	cmdlineFacts, cmdlineDeployed, activationByFlags = hercules.Registry.AddFlags(rootFlags)
	rootCmd.SetUsageFunc(formatUsage)
	rootCmd.AddCommand(versionCmd)
	versionCmd.SetUsageFunc(versionCmd.UsageFunc())
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
