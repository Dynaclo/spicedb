package indexer

import (
	"fmt"
	"github.com/hmdsefi/gograph"
	"math/rand"
	"os"
)

// Fully Dynamic Transitive Closure Index
type FullDynTCIndex interface {
	NewIndex(graph gograph.Graph[string])

	InsertEdge(src string, dst string) error
	DeleteEdge(src string, dst string) error

	CheckReachability(src string, dst string) (bool, error)
}

type SVK struct {
	Graph        gograph.Graph[string]
	ReverseGraph gograph.Graph[string]
	SV           *gograph.Vertex[string]
	R_Plus       map[string]bool //store all vertices reachable from SV
	R_Minus      map[string]bool //store all vertices that can reach SV
	numReads     int
}

const opsThreshold = 50

func (algo *SVK) updateSvkOptionally() {
	if algo.numReads == -1 {
		algo.NewIndex(algo.Graph)
	}
	if algo.numReads > opsThreshold {
		algo.numReads = 0
		algo.pickSv()
		algo.recalculateIndex()
	}
}

// maybe have a Init() fn return pointer to Graph object to which
// vertices are added instead of taking in graph as param which casues huge copy
// ok since it is a inti step tho ig
func (algo *SVK) NewIndex(graph gograph.Graph[string]) {
	if graph == nil {
		graph = gograph.New[string](gograph.Directed())
		src := gograph.NewVertex("empty:0")
		dest := gograph.NewVertex("empty:1")
		_, _ = graph.AddEdge(src, dest)
	}
	algo.Graph = graph

	print(algo.Graph)
	//make reverse DiGraph
	algo.reverseGraph()

	algo.pickSv()

	algo.recalculateIndex()
}

func (algo *SVK) reverseGraph() {
	algo.ReverseGraph = gograph.New[string](gograph.Directed())
	for _, e := range algo.Graph.AllEdges() {
		algo.ReverseGraph.AddEdge(e.Destination(), e.Source())
	}
}

func (algo *SVK) recalculateIndex() {
	vertices := algo.Graph.GetAllVertices()
	//initialize R_Plus
	algo.R_Plus = make(map[string]bool)
	for _, v := range vertices {
		algo.R_Plus[v.Label()] = false
	}
	algo.recomputeRPlus()

	//initialize R_Minus
	algo.R_Minus = make(map[string]bool)
	for _, v := range vertices {
		algo.R_Minus[v.Label()] = false
	}
	algo.recomputeRMinus()
}

func (algo *SVK) pickSv() {
	vertices := algo.Graph.GetAllVertices()
	randomIndex := rand.Intn(len(vertices))
	algo.SV = vertices[randomIndex]

	//make sure this is not a isolated vertex and repick if it is
	for algo.SV.Degree() == 0 {
		randomIndex = rand.Intn(len(vertices))
		algo.SV = vertices[randomIndex]
	}
	fmt.Println(algo.SV.Label(), " chosen as SV")
}

func (algo *SVK) recomputeRPlus() {
	// Initialize a queue for BFS
	queue := []*gograph.Vertex[string]{algo.SV}

	// Reset R_Plus to mark all vertices as not reachable
	for key := range algo.R_Plus {
		algo.R_Plus[key] = false
	}

	// Start BFS
	for len(queue) > 0 {

		current := queue[0]
		queue = queue[1:]

		algo.R_Plus[current.Label()] = true

		// Enqueue all neighbors (vertices connected by an outgoing edge)
		for _, edge := range algo.Graph.AllEdges() {
			if edge.Source().Label() == current.Label() {
				destVertex := edge.Destination()
				if !algo.R_Plus[destVertex.Label()] {
					queue = append(queue, destVertex)
				}
			}
		}
	}
}

func (algo *SVK) recomputeRMinus() {

	queue := []*gograph.Vertex[string]{algo.SV}

	for key := range algo.R_Minus {
		algo.R_Minus[key] = false
	}

	// Start BFS
	for len(queue) > 0 {

		current := queue[0]
		queue = queue[1:]

		algo.R_Minus[current.Label()] = true

		for _, edge := range algo.ReverseGraph.AllEdges() {
			if edge.Source().Label() == current.Label() {
				destVertex := edge.Destination()
				if !algo.R_Minus[destVertex.Label()] {
					queue = append(queue, destVertex)
				}
			}
		}
	}
}

func (algo *SVK) insertEdge(src string, dst string) error {
	srcVertex := algo.Graph.GetVertexByID(src)
	if srcVertex == nil {
		srcVertex = gograph.NewVertex(src)
		algo.Graph.AddVertex(srcVertex)
		algo.R_Plus[src] = false
		algo.R_Minus[src] = false
	}

	dstVertex := algo.Graph.GetVertexByID(dst)
	if dstVertex == nil {
		dstVertex = gograph.NewVertex(dst)
		algo.Graph.AddVertex(dstVertex)
		algo.R_Plus[dst] = false
		algo.R_Minus[dst] = false
	}

	algo.Graph.AddEdge(srcVertex, dstVertex)
	algo.ReverseGraph.AddEdge(dstVertex, srcVertex)

	//update R+ and R-
	//TODO: Make this not be a full recompute using an SSR data structure
	algo.recomputeRPlus()
	algo.recomputeRMinus()

	fmt.Printf("Successfully inserted edge %s -> %s\n", src, dst)
	return nil
}

func (algo *SVK) DeleteEdge(src string, dst string) error {
	//TODO: Check if either src or dst are isolated after edgedelete and delete the node if they are not schema nodes
	//TODO: IF deleted node is SV or if SV gets isolated repick SV

	srcVertex := algo.Graph.GetVertexByID(src)
	dstVertex := algo.Graph.GetVertexByID(dst)

	edge := algo.Graph.GetEdge(srcVertex, dstVertex)
	algo.Graph.RemoveEdges(edge)

	rev_edge := algo.ReverseGraph.GetEdge(dstVertex, srcVertex)
	algo.ReverseGraph.RemoveEdges(rev_edge)
	//TODO: Add error handling here for if vertex or edge does not exist

	//TODO: Make this not be a full recompute using an SSR data structure
	algo.recomputeRPlus()
	algo.recomputeRMinus()
	return nil
}

// Directed BFS implementation
func directedBFS(graph gograph.Graph[string], src string, dst string) (bool, error) {

	queue := []*gograph.Vertex[string]{}

	visited := make(map[string]bool)

	// Start BFS from the source vertex
	startVertex := graph.GetVertexByID(src)
	if startVertex == nil {
		return false, fmt.Errorf("source vertex %s not found", src)
	}
	queue = append(queue, startVertex)
	visited[src] = true

	for len(queue) > 0 {
		// Dequeue the front of the queue
		currentVertex := queue[0]
		queue = queue[1:]

		// If we reach the destination vertex return true
		if currentVertex.Label() == dst {
			return true, nil
		}

		// Get all edges from the current vertex
		for _, edge := range graph.AllEdges() {
			// Check if the edge starts from the current vertex (directed edge)
			if edge.Source().Label() == currentVertex.Label() {
				nextVertex := edge.Destination()
				if !visited[nextVertex.Label()] {
					visited[nextVertex.Label()] = true
					queue = append(queue, nextVertex)
				}
			}
		}
	}

	// If we exhaust the queue without finding the destination, return false
	return false, nil
}

func (algo *SVK) checkReachability(src string, dst string) (bool, error) {
	algo.updateSvkOptionally()
	svLabel := algo.SV.Label()

	//if src is support vertex
	if svLabel == src {
		fmt.Println("[CheckReachability][Resolved] Src vertex is SV")
		return algo.R_Plus[dst], nil
	}

	//if dest is support vertex
	if svLabel == dst {
		fmt.Println("[CheckReachability][Resolved] Dst vertex is SV")
		return algo.R_Minus[dst], nil
	}

	//try to apply O1
	if algo.R_Minus[src] == true && algo.R_Plus[dst] == true {
		fmt.Println("[CheckReachability][Resolved] Using O1")
		return true, nil
	}

	//try to apply O2
	if algo.R_Plus[src] == true && algo.R_Plus[dst] == false {
		fmt.Println("[CheckReachability][Resolved] Using O2")
		return false, nil
	}

	//try to apply O3
	if algo.R_Minus[src] == false && algo.R_Minus[dst] == true {
		fmt.Println("[CheckReachability][Resolved] Using O3")
		return false, nil
	}

	//if all else fails, fallback to BFS
	fmt.Println("[CheckReachability][Resolved] Fallback to BFS")
	bfs, err := directedBFS(algo.Graph, src, dst)
	if err != nil {
		return false, err
	}

	if bfs == true {
		return true, nil
	}
	algo.numReads++
	return false, nil
}

func generateDotFile(graph gograph.Graph[string], filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString("digraph G {\n")
	if err != nil {
		return err
	}

	for _, edge := range graph.AllEdges() {
		line := fmt.Sprintf("  \"%s\" -> \"%s\";\n", edge.Source().Label(), edge.Destination().Label())
		_, err := file.WriteString(line)
		if err != nil {
			return err
		}
	}

	_, err = file.WriteString("}\n")
	return err
}
