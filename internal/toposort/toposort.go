package toposort

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
)

// Reworked from https://github.com/philopon/go-toposort

// Graph represents a directed acyclic graph.
type Graph struct {
	// Outgoing connections for every node.
	outputs map[string]map[string]struct{}
	// How many parents each node has.
	inputs map[string]int
	sorter func([]string)
}

// NewGraph initializes a new Graph.
func NewGraph() *Graph {
	return &Graph{
		inputs:  map[string]int{},
		outputs: map[string]map[string]struct{}{},
	}
}

func NewGraphWithSorter(sorter func([]string)) *Graph {
	g := NewGraph()
	g.sorter = sorter
	return g
}

func (g *Graph) Sort(values []string) {
	if g.sorter == nil {
		sort.Strings(values)
	} else {
		g.sorter(values)
	}
}

// AddNode inserts a new node into the graph.
func (g *Graph) AddNode(name string) bool {
	if _, exists := g.outputs[name]; exists {
		return false
	}
	g.outputs[name] = map[string]struct{}{}
	g.inputs[name] = 0
	return true
}

// AddNodes inserts multiple nodes into the graph at once.
func (g *Graph) AddNodes(names ...string) bool {
	for _, name := range names {
		if ok := g.AddNode(name); !ok {
			return false
		}
	}
	return true
}

// AddEdge inserts the link from "from" node to "to" node.
func (g *Graph) AddEdge(from, to string) int {
	m, ok := g.outputs[from]
	if !ok {
		return 0
	}

	m[to] = struct{}{}
	ni := g.inputs[to] + 1
	g.inputs[to] = ni

	return ni
}

func (g *Graph) InputCount(name string) (int, bool) {
	n, ok := g.inputs[name]
	return n, ok
}

// RemoveEdge deletes the link from "from" node to "to" node.
// Call ReindexNode(from) after you finish modifying the edges.
func (g *Graph) RemoveEdge(from, to string) bool {
	if _, ok := g.outputs[from]; !ok {
		return false
	}
	delete(g.outputs[from], to)
	g.inputs[to]--
	return true
}

// Toposort sorts the nodes in the graph in topological order.
func (g *Graph) Toposort() ([]string, bool) {
	result := make([]string, 0, len(g.outputs))
	queue := make([]string, 0, len(g.outputs))
	counters := make(map[string]int, len(g.inputs))

	for n := range g.outputs {
		if g.inputs[n] == 0 {
			queue = append(queue, n)
		}
	}
	g.Sort(queue)

	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		result = append(result, n)

		queueLen := len(queue)
		for k := range g.outputs[n] {
			switch c, ok := counters[k]; {
			case !ok:
				c = g.inputs[k]
				if c == 1 {
					break
				}
				fallthrough
			case c != 1:
				counters[k] = c - 1
				continue
			}
			counters[k] = 0
			queue = append(queue, k)
		}

		g.Sort(queue[queueLen:])
	}

	return result, len(result) == len(g.inputs)
}

type NodePosition struct {
	Level int
	Index int
}

type nodePosSorter struct {
	nodes     []string
	positions map[string]NodePosition
}

func (v nodePosSorter) Len() int {
	return len(v.nodes)
}

func (v nodePosSorter) Less(i, j int) bool {
	return v.positions[v.nodes[i]].Index < v.positions[v.nodes[j]].Index
}

func (v nodePosSorter) Swap(i, j int) {
	v.nodes[i], v.nodes[j] = v.nodes[j], v.nodes[i]
}

func SortByNodeIndex(nodes []string, positions map[string]NodePosition) {
	sort.Sort(nodePosSorter{nodes: nodes, positions: positions})
}

// BreadthSort sorts the nodes in the graph in BFS order.
func (g *Graph) BreadthSort() map[string]NodePosition {
	S := make([]string, 0, len(g.outputs))

	result := map[string]NodePosition{}
	levels := map[string]int{}

	for n := range g.outputs {
		if g.inputs[n] == 0 {
			S = append(S, n)
		}
	}

	for len(S) > 0 {
		node := S[0]
		S = S[1:]
		if _, exists := result[node]; !exists {
			level := levels[node]
			result[node] = NodePosition{
				Level: level,
				Index: len(result),
			}
			level++
			for child := range g.outputs[node] {
				S = append(S, child)
				levels[child] = level
			}
		}
	}

	return result
}

// FindCycle returns the cycle in the graph which contains "seed" node.
func (g *Graph) FindCycle(seed string) []string {
	type edge struct {
		node   string
		parent string
	}
	S := make([]edge, 0, len(g.outputs))
	S = append(S, edge{seed, ""})
	visited := map[string]string{}
	for len(S) > 0 {
		e := S[0]
		S = S[1:]
		if parent, exists := visited[e.node]; !exists || parent == "" {
			visited[e.node] = e.parent
			for child := range g.outputs[e.node] {
				S = append(S, edge{child, e.node})
			}
		}
		if e.node == seed && e.parent != "" {
			var result []string
			node := e.parent
			for node != seed {
				result = append(result, node)
				node = visited[node]
			}
			result = append(result, seed)
			// reverse
			for left, right := 0, len(result)-1; left < right; left, right = left+1, right-1 {
				result[left], result[right] = result[right], result[left]
			}
			return result
		}
	}
	return []string{}
}

// FindParents returns the other ends of incoming edges.
func (g *Graph) FindParents(to string) (result []string) {
	for node, children := range g.outputs {
		if _, exists := children[to]; exists {
			result = append(result, node)
		}
	}
	return result
}

// FindChildren returns the other ends of outgoing edges.
func (g *Graph) FindChildren(from string) (result []string) {
	for child := range g.outputs[from] {
		result = append(result, child)
	}
	return result
}

// Serialize outputs the graph in Graphviz format.
func (g *Graph) Serialize(sorted []string) string {
	node2index := map[string]int{}
	for index, node := range sorted {
		node2index[node] = index
	}
	var buffer bytes.Buffer
	buffer.WriteString("digraph Hercules {\n")
	var nodesFrom []string
	for nodeFrom := range g.outputs {
		nodesFrom = append(nodesFrom, nodeFrom)
	}
	g.Sort(nodesFrom)
	for _, nodeFrom := range nodesFrom {
		var links []string
		for nodeTo := range g.outputs[nodeFrom] {
			links = append(links, nodeTo)
		}
		g.Sort(links)
		for _, nodeTo := range links {
			buffer.WriteString(fmt.Sprintf("  \"%d %s\" -> \"%d %s\"\n",
				node2index[nodeFrom], nodeFrom, node2index[nodeTo], nodeTo))
		}
	}
	buffer.WriteString("}")
	return buffer.String()
}

// DebugDump converts the graph to a string. As the name suggests, useful for debugging.
func (g *Graph) DebugDump() string {
	S := make([]string, 0, len(g.outputs))
	for n := range g.outputs {
		if g.inputs[n] == 0 {
			S = append(S, n)
		}
	}
	g.Sort(S)
	var buffer bytes.Buffer
	buffer.WriteString(strings.Join(S, " ") + "\n")
	keys := []string(nil)
	vals := map[string][]string{}
	for key, val1 := range g.outputs {
		val2 := make([]string, 0, len(val1))
		for name := range val1 {
			val2 = append(val2, name)
		}
		keys = append(keys, key)
		vals[key] = val2
	}
	g.Sort(keys)
	for _, key := range keys {
		buffer.WriteString(fmt.Sprintf("%s %d = ", key, g.inputs[key]))
		outs := vals[key]
		buffer.WriteString(strings.Join(outs, " ") + "\n")
	}
	return buffer.String()
}

func (g *Graph) HasChildren(name string) bool {
	return len(g.outputs[name]) > 0
}
