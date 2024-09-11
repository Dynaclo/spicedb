package indexer

import (
	"fmt"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/hmdsefi/gograph"
	"math/rand"
	"os"
)

var Index SV1

// Fully Dynamic Transitive Closure Index
type FullDynTCIndex interface {
	NewIndex(graph gograph.Graph[string])

	InsertEdge(src string, dst string) error
	DeleteEdge(src string, dst string) error

	CheckReachability(src string, dst string) (bool, error)
}

type SV1 struct {
	Graph        gograph.Graph[string]
	ReverseGraph gograph.Graph[string]
	SV           *gograph.Vertex[string]
	R_Plus       map[string]bool //store all vertices reachable from SV
	R_Minus      map[string]bool //store all vertices that can reach SV
}

var graph gograph.Graph[string]

func makeNodeNameFromObjectRelationPair(relation *corev1.ObjectAndRelation) string {
	return relation.Namespace + "#" + relation.ObjectId
}

func createVertexIfNeeded(relation *corev1.ObjectAndRelation) *gograph.Vertex[string] {
	if graph == nil {
		graph = gograph.New[string](gograph.Directed())
	}
	name := makeNodeNameFromObjectRelationPair(relation)
	vtx := graph.GetVertexByID(name)
	if graph.GetVertexByID(name) == nil {
		vtx = gograph.NewVertex[string](name)
		graph.AddVertex(vtx)
	}
	return vtx

}

func AddEdge(tuple *corev1.RelationTuple) {
	src := createVertexIfNeeded(tuple.ResourceAndRelation)
	dest := createVertexIfNeeded(tuple.Subject)
	_, err := graph.AddEdge(src, dest)
	if err != nil {
		panic(err)
	}
}

func NewIndex() {
	sv := SV1{}
	sv.NewIndex(graph)
	Index = sv
}

// maybe have a Init() fn return pointer to Graph object to which
// vertices are added instead of taking in graph as param which casues huge copy
// ok since it is a inti step tho ig
func (algo *SV1) NewIndex(graph gograph.Graph[string]) {
	algo.Graph = graph
	fmt.Printf("hiiii")
	print(algo.Graph)
	//make reverse DiGraph
	algo.ReverseGraph = gograph.New[string](gograph.Directed())
	for _, e := range algo.Graph.AllEdges() {
		algo.ReverseGraph.AddEdge(e.Destination(), e.Source())
	}

	//select support vertex
	//TODO: implement getting random vertex in the library itself
	vertices := algo.Graph.GetAllVertices()
	randomIndex := rand.Intn(len(vertices))
	algo.SV = vertices[randomIndex]

	//make sure this is not a isolated vertex and repick if it is
	for algo.SV.Degree() == 0 {
		randomIndex = rand.Intn(len(vertices))
		algo.SV = vertices[randomIndex]
	}
	fmt.Println(algo.SV.Label(), " chosen as SV")

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

func (algo *SV1) recomputeRPlus() {
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

func (algo *SV1) recomputeRMinus() {

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

func (algo *SV1) InsertEdge(src string, dst string) error {
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
	return nil
}

func (algo *SV1) DeleteEdge(src string, dst string) error {
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

func (algo *SV1) CheckReachability(src string, dst string) (bool, error) {
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

func (algo *SV1) DumpGraph() {
	err := generateDotFile(algo.Graph, "graph.dot")
	if err != nil {
		panic(err)
	}
}
