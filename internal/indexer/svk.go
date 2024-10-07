package indexer

import (
	"errors"
	"fmt"
	"github.com/Dynaclo/Onyx"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/hmdsefi/gograph"
	"math/rand"
	"os"
	"sync"
	"time"
)

// Fully Dynamic Transitive Closure Index
type FullDynTCIndex interface {
	NewIndex(graph gograph.Graph[string])

	InsertEdge(src string, dst string) error
	DeleteEdge(src string, dst string) error

	CheckReachability(src string, dst string) (bool, error)
}

type RPair struct {
	R_Plus  map[string]bool
	R_Minus map[string]bool
}
type SVK struct {
	Graph         *Onyx.Graph
	ReverseGraph  *Onyx.Graph
	SV            string
	RPairMutex    sync.RWMutex
	RPair         *RPair
	numReads      int
	blueQueue     *WriteQueue
	greenQueue    *WriteQueue
	CurQueueLock  sync.RWMutex
	CurrentQueue  *WriteQueue
	lastUpdated   time.Time
	lastUpdatedMu sync.Mutex
}

type Operation struct {
	OpType      string // todo: make enum
	Source      string
	Destination string
	Relation    string
}

const opsThreshold = 50
const timeThresholdMillis = 500 * time.Millisecond

func (algo *SVK) applyWrites() {
	if !algo.lastUpdatedMu.TryLock() {
		return
	}
	defer algo.lastUpdatedMu.Unlock()
	prevQueue := algo.CurrentQueue
	algo.CurQueueLock.Lock()
	if algo.CurrentQueue == algo.blueQueue {
		algo.CurrentQueue = algo.greenQueue
	} else {
		algo.CurrentQueue = algo.blueQueue
	}
	algo.CurQueueLock.Unlock()
	operations := prevQueue.DrainQueue()

	for _, operation := range operations {
		if operation.OpType == "insert" {
			err := algo.insertEdge(operation.Source, operation.Destination)
			if err != nil {
				log.Warn().Err(err)
			}
		} else if operation.OpType == "delete" {
			//err := algo.dele
		}
	}
	algo.pickSv()
	algo.recompute()
	algo.lastUpdated = time.Now()
}

func (algo *SVK) updateSvkOptionally() {
	if algo.CurrentQueue.Available != algo.CurrentQueue.Size {
		// we have writes in queue
		if algo.lastUpdated.Add(timeThresholdMillis).Before(time.Now()) {
			// we must apply writes
			algo.applyWrites()
		}
	}
	//if algo.numReads > opsThreshold {
	//	algo.numReads = 0
	//	algo.pickSv()
	//	algo.initializeRplusAndRminusAtStartupTime()
	//}
}

// maybe have a Init() fn return pointer to Graph object to which
// vertices are added instead of taking in graph as param which casues huge copy
// ok since it is a inti step tho ig
func (algo *SVK) NewIndex(graph *Onyx.Graph) error {
	if graph == nil {
		graph, err := Onyx.NewGraph("./onyx-graph", false)
		if err != nil {
			return err
		}

		err = graph.AddEdge("empty:0", "empty:1", nil)
		if err != nil {
			return err
		}
	}
	algo.Graph = graph

	print(algo.Graph)
	//make reverse DiGraph
	algo.reverseGraph()

	algo.pickSv()

	algo.initializeRplusAndRminusAtStartupTime()
}

func (algo *SVK) reverseGraph() error {
	var err error
	algo.ReverseGraph, err = Onyx.NewGraph("./onyx-graph-rev", false)
	if err != nil {
		return err
	}
	for _, e := range algo.Graph.AllEdges() {
		algo.ReverseGraph.AddEdge(e.Destination(), e.Source())
	}
}

func (algo *SVK) initializeRplusAndRminusAtStartupTime() {
	vertices := algo.Graph.GetAllVertices()
	//initialize R_Plus
	_map := make(map[string]bool)
	algo.RPairMutex.Lock()
	algo.RPair.R_Plus = _map
	for _, v := range vertices {
		algo.RPair.R_Plus[v.Label()] = false
	}
	//initialize R_Minus
	_map = make(map[string]bool)
	algo.RPair.R_Minus = _map
	for _, v := range vertices {
		algo.RPair.R_Minus[v.Label()] = false
	}
	algo.RPairMutex.Unlock()
	algo.recompute()
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

func (algo *SVK) recompute() {
	copyOfRpair := *algo.RPair
	wg := sync.WaitGroup{}
	wg.Add(2)
	go algo.recomputeRPlus(&copyOfRpair, &wg)
	go algo.recomputeRMinus(&copyOfRpair, &wg)
	wg.Wait()
	algo.RPairMutex.Lock()
	defer algo.RPairMutex.Unlock()
	algo.RPair = &copyOfRpair
}

func (algo *SVK) recomputeRPlus(pair *RPair, wg *sync.WaitGroup) {
	defer wg.Done()
	// Initialize a queue for BFS
	queue := []*gograph.Vertex[string]{algo.SV}

	// Reset R_Plus to mark all vertices as not reachable
	for key := range pair.R_Plus {
		pair.R_Plus[key] = false
	}

	// Start BFS
	for len(queue) > 0 {

		current := queue[0]
		queue = queue[1:]

		pair.R_Plus[current.Label()] = true

		// Enqueue all neighbors (vertices connected by an outgoing edge)
		for _, edge := range algo.Graph.AllEdges() {
			if edge.Source().Label() == current.Label() {
				destVertex := edge.Destination()
				if !pair.R_Plus[destVertex.Label()] {
					queue = append(queue, destVertex)
				}
			}
		}
	}
}

func (algo *SVK) recomputeRMinus(pair *RPair, wg *sync.WaitGroup) {
	defer wg.Done()
	queue := []*gograph.Vertex[string]{algo.SV}

	for key := range pair.R_Minus {
		pair.R_Minus[key] = false
	}

	// Start BFS
	for len(queue) > 0 {

		current := queue[0]
		queue = queue[1:]

		pair.R_Minus[current.Label()] = true

		for _, edge := range algo.ReverseGraph.AllEdges() {
			if edge.Source().Label() == current.Label() {
				destVertex := edge.Destination()
				if !pair.R_Minus[destVertex.Label()] {
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
	}

	dstVertex := algo.Graph.GetVertexByID(dst)
	if dstVertex == nil {
		dstVertex = gograph.NewVertex(dst)
		algo.Graph.AddVertex(dstVertex)
	}

	algo.Graph.AddEdge(srcVertex, dstVertex)
	algo.ReverseGraph.AddEdge(dstVertex, srcVertex)

	////TODO: Make this not be a full recompute using an SSR data structure
	//algo.recompute()

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
	algo.RPairMutex.Lock()
	defer algo.RPairMutex.Unlock()
	algo.recompute()
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
		return algo.RPair.R_Plus[dst], nil
	}

	if !algo.RPairMutex.TryRLock() {
		return false, errors.New("[CheckReachability][Unresoled] failed to get RLock")
	}
	defer algo.RPairMutex.RUnlock()

	//if dest is support vertex
	if svLabel == dst {
		fmt.Println("[CheckReachability][Resolved] Dst vertex is SV")
		return algo.RPair.R_Minus[dst], nil
	}

	//try to apply O1
	if algo.RPair.R_Minus[src] == true && algo.RPair.R_Plus[dst] == true {
		fmt.Println("[CheckReachability][Resolved] Using O1")
		return true, nil
	}

	//try to apply O2
	if algo.RPair.R_Plus[src] == true && algo.RPair.R_Plus[dst] == false {
		fmt.Println("[CheckReachability][Resolved] Using O2")
		return false, nil
	}

	//try to apply O3
	if algo.RPair.R_Minus[src] == false && algo.RPair.R_Minus[dst] == true {
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
