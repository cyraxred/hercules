package core

import (
	"testing"

	"github.com/cyraxred/hercules/internal/test"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/stretchr/testify/assert"
)

type testForkPipelineItem struct {
	NoopMerger
	Mutable   map[int]bool
	Immutable string
}

func (item *testForkPipelineItem) Name() string {
	return "Test"
}

func (item *testForkPipelineItem) Provides() []string {
	return []string{"test"}
}

func (item *testForkPipelineItem) Requires() []string {
	return []string{}
}

func (item *testForkPipelineItem) Configure(facts map[string]interface{}) error {
	return nil
}

func (item *testForkPipelineItem) ConfigureUpstream(facts map[string]interface{}) error {
	return nil
}

func (item *testForkPipelineItem) ListConfigurationOptions() []ConfigurationOption {
	return nil
}

func (item *testForkPipelineItem) Flag() string {
	return "mytest"
}

func (item *testForkPipelineItem) Features() []string {
	return nil
}

func (item *testForkPipelineItem) Initialize(repository *git.Repository) error {
	item.Mutable = map[int]bool{}
	return nil
}

func (item *testForkPipelineItem) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	return map[string]interface{}{"test": "foo"}, nil
}

func (item *testForkPipelineItem) Fork(n int) []PipelineItem {
	return ForkCopyPipelineItem(item, n)
}

func TestForkCopyPipelineItem(t *testing.T) {
	origin := &testForkPipelineItem{}
	origin.Initialize(nil)
	origin.Mutable[2] = true
	origin.Immutable = "before"
	clone := origin.Fork(1)[0].(*testForkPipelineItem)
	origin.Immutable = "after"
	origin.Mutable[1] = true
	assert.True(t, clone.Mutable[1])
	assert.True(t, clone.Mutable[2])
	assert.Equal(t, "before", clone.Immutable)
}

func TestInsertHibernateBoot(t *testing.T) {
	plan := []runAction{
		{runActionEmerge, nil, nil, []int{1, 2}},
		{runActionEmerge, nil, nil, []int{3}},
		{runActionCommit, nil, nil, []int{3}},
		{runActionCommit, nil, nil, []int{3}},
		{runActionCommit, nil, nil, []int{1}},
		{runActionFork, nil, nil, []int{2, 4}},
		{runActionCommit, nil, nil, []int{3}},
		{runActionCommit, nil, nil, []int{3}},
		{runActionDelete, nil, nil, []int{1}},
		{runActionMerge, nil, nil, []int{2, 4}},
	}
	plan = insertHibernateBoot(plan, 2)
	assert.Equal(t, []runAction{
		{runActionEmerge, nil, nil, []int{1, 2}},
		{runActionHibernate, nil, nil, []int{1, 2}},
		{runActionEmerge, nil, nil, []int{3}},
		{runActionCommit, nil, nil, []int{3}},
		{runActionCommit, nil, nil, []int{3}},
		{runActionBoot, nil, nil, []int{1}},
		{runActionCommit, nil, nil, []int{1}},
		{runActionBoot, nil, nil, []int{2}},
		{runActionFork, nil, nil, []int{2, 4}},
		{runActionHibernate, nil, nil, []int{2, 4}},
		{runActionCommit, nil, nil, []int{3}},
		{runActionCommit, nil, nil, []int{3}},
		{runActionDelete, nil, nil, []int{1}},
		{runActionBoot, nil, nil, []int{2, 4}},
		{runActionMerge, nil, nil, []int{2, 4}},
	}, plan)
}

func TestRunActionString(t *testing.T) {
	c, _ := test.Repository.CommitObject(plumbing.NewHash("c1002f4265a704c703207fafb95f1d4255bfae1a"))
	ra := runAction{runActionCommit, c, nil, nil}
	assert.Equal(t, ra.String(), "c1002f4")
	ra = runAction{runActionFork, nil, nil, []int{1, 2, 5}}
	assert.Equal(t, ra.String(), "fork^3")
	ra = runAction{runActionMerge, nil, nil, []int{1, 2, 5}}
	assert.Equal(t, ra.String(), "merge^3")
	ra = runAction{runActionEmerge, nil, nil, nil}
	assert.Equal(t, ra.String(), "emerge")
	ra = runAction{runActionDelete, nil, nil, nil}
	assert.Equal(t, ra.String(), "delete")
	ra = runAction{runActionHibernate, nil, nil, nil}
	assert.Equal(t, ra.String(), "hibernate")
	ra = runAction{runActionBoot, nil, nil, nil}
	assert.Equal(t, ra.String(), "boot")
}
