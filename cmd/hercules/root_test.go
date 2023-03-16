package main

import (
	"github.com/cyraxred/hercules/internal/core"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/stretchr/testify/assert"
)

func TestLoadGitRepository(t *testing.T) {
	repo, repoFeature := loadRepository("https://github.com/src-d/hercules", "", true, "")
	assert.NotNil(t, repo)
	assert.Equal(t, repoFeature, core.FeatureGitCommits)
}

func TestLoadLocalRepository(t *testing.T) {
	tempdir, err := ioutil.TempDir("", "hercules-")
	assert.Nil(t, err)
	defer func() { _ = os.RemoveAll(tempdir) }()

	backend := filesystem.NewStorage(osfs.New(tempdir), cache.NewObjectLRUDefault())
	cloneOptions := &git.CloneOptions{URL: "https://github.com/src-d/hercules"}
	_, err = git.Clone(backend, nil, cloneOptions)
	assert.Nil(t, err)
	if err != nil {
		assert.FailNow(t, "filesystem.NewStorage")
	}

	repo, repoFeature := loadRepository(tempdir, "", true, "")
	assert.NotNil(t, repo)
	assert.Equal(t, repoFeature, core.FeatureGitCommits)
}

func TestLoadSivaRepository(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	sivafile := filepath.Join(filepath.Dir(filename), "test_data", "hercules.siva")
	repo, repoFeature := loadRepository(sivafile, "", true, "")
	assert.NotNil(t, repo)
	assert.Equal(t, repoFeature, core.FeatureGitCommits)

	assert.Panics(t, func() { loadRepository("https://github.com/src-d/porn", "", true, "") })
	assert.Panics(t, func() { loadRepository(filepath.Dir(filename), "", true, "") })
	assert.Panics(t, func() { loadRepository("/xxx", "", true, "") })
}

func TestLoadStubRepository(t *testing.T) {
	repo, repoFeature := loadRepository("-", "", true, "")
	assert.NotNil(t, repo)
	assert.Equal(t, repoFeature, core.FeatureGitStub)
}
