package indexer

import (
	"errors"
	"fmt"
	"github.com/Dynaclo/Onyx"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/hmdsefi/gograph"
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
		graph, err := Onyx.NewGraph("./onyx-graph", false || IN_MEMORY_GLOBAL)
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

	return nil
}

func (algo *SVK) reverseGraph() error {
	var err error
	algo.ReverseGraph, err = Onyx.NewGraph("./onyx-graph-rev", false || IN_MEMORY_GLOBAL)
	if err != nil {
		return err
	}

	revGraphTxn := algo.ReverseGraph.DB.NewTransaction(true)
	defer revGraphTxn.Discard()

	err = algo.Graph.IterAllEdges(func(src string, dst string) error {
		algo.ReverseGraph.AddEdge(dst, src, revGraphTxn)
		return nil
	}, 25, nil)
	//do NOT pass revGraphTxn to This IterAllEdges, they are 2 DIFFERENT badger stores

	if err != nil {
		return err
	}

	err = revGraphTxn.Commit()
	return err
}

func (algo *SVK) initializeRplusAndRminusAtStartupTime() {
	algo.RPairMutex.Lock()
	//initialize R_Plus
	algo.RPair.R_Plus = make(map[string]bool)
	//initialize R_Minus
	algo.RPair.R_Minus = make(map[string]bool)
	algo.RPairMutex.Unlock()
	algo.recompute()
}

func (algo *SVK) pickSv() error {
	vertex, err := algo.Graph.PickRandomVertex(nil)
	if err != nil {
		return err
	}
	algo.SV = vertex

	//make sure this is not a isolated vertex and repick if it is
	outDegree, err := algo.Graph.OutDegree(algo.SV, nil)
	if err != nil {
		return err
	}
	inDegree, err := algo.ReverseGraph.OutDegree(algo.SV, nil)
	if err != nil {
		return err
	}
	for outDegree == 0 && inDegree == 0 {
		vertex, err = algo.Graph.PickRandomVertex(nil)
		if err != nil {
			return err
		}
		algo.SV = vertex

		outDegree, err = algo.Graph.OutDegree(algo.SV, nil)
		if err != nil {
			return err
		}
		inDegree, err = algo.ReverseGraph.OutDegree(algo.SV, nil)
		if err != nil {
			return err
		}
	}
	fmt.Println(algo.SV, " chosen as SV")
	return nil
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

func (algo *SVK) recomputeRPlus(pair *RPair, wg *sync.WaitGroup) error {
	defer wg.Done()
	// Initialize a queue for BFS
	queue := []string{algo.SV}

	// Reset R_Plus to mark all vertices as not reachable
	for key := range pair.R_Plus {
		delete(pair.R_Plus, key)
	}

	// Start BFS
	for len(queue) > 0 {

		current := queue[0]
		queue = queue[1:]

		pair.R_Plus[current] = true

		// Enqueue all neighbors (vertices connected by an outgoing edge)
		neighbors, err := algo.Graph.GetEdges(current, nil)
		if err != nil {
			return err
		}
		for destVertex, _ := range neighbors {
			if !pair.R_Plus[destVertex] {
				queue = append(queue, destVertex)
			}
		}
	}

	for k, v := range pair.R_Plus {
		fmt.Println("[R+] ", k, ": ", v)
	}
	return nil
}

func (algo *SVK) recomputeRMinus(pair *RPair, wg *sync.WaitGroup) error {
	defer wg.Done()
	queue := []string{algo.SV}

	for key := range pair.R_Minus {
		delete(pair.R_Minus, key)
	}

	// Start BFS
	for len(queue) > 0 {

		current := queue[0]
		queue = queue[1:]

		pair.R_Minus[current] = true

		neighbors, err := algo.ReverseGraph.GetEdges(current, nil)
		if err != nil {
			return err
		}
		for destVertex, _ := range neighbors {
			if !pair.R_Minus[destVertex] {
				queue = append(queue, destVertex)
			}
		}
	}

	fmt.Println("========Printing R Minus=========")
	for k, v := range pair.R_Minus {
		fmt.Println("[R-] ", k, ": ", v)
	}
	return nil
}

func (algo *SVK) insertEdge(src string, dst string) error {
	err := algo.Graph.AddEdge(src, dst, nil)
	if err != nil {
		return err
	}
	err = algo.ReverseGraph.AddEdge(dst, src, nil)
	if err != nil {
		return err
	}

	////TODO: Make this not be a full recompute using an SSR data structure
	//algo.recompute()

	fmt.Printf("Successfully inserted edge %s -> %s\n", src, dst)
	return nil
}

func (algo *SVK) DeleteEdge(src string, dst string) error {
	//TODO: Check if either src or dst are isolated after edgedelete and delete the node if they are not schema nodes
	//TODO: IF deleted node is SV or if SV gets isolated repick SV

	err := algo.Graph.RemoveEdge(src, dst, nil)
	if err != nil {
		return err
	}

	err = algo.ReverseGraph.RemoveEdge(dst, src, nil)
	if err != nil {
		return err
	}
	//TODO: Add error handling here for if vertex or edge does not exist

	//TODO: Make this not be a full recompute using an SSR data structure
	algo.RPairMutex.Lock()
	defer algo.RPairMutex.Unlock()
	algo.recompute()
	return nil
}

// Directed BFS implementation
func directedBFS(graph *Onyx.Graph, src string, dst string) (bool, error) {

	queue := []string{}

	visited := make(map[string]bool)

	// Start BFS from the source vertex
	queue = append(queue, src)
	visited[src] = true

	for len(queue) > 0 {
		// Dequeue the front of the queue
		currentVertex := queue[0]
		queue = queue[1:]

		// If we reach the destination vertex return true
		if currentVertex == dst {
			return true, nil
		}

		neighbors, err := graph.GetEdges(currentVertex, nil)
		if err != nil {
			return false, err
		}
		// Get all edges from the current vertex
		for nextVertex, _ := range neighbors {
			// Check if the edge starts from the current vertex (directed edge)
			if !visited[nextVertex] {
				visited[nextVertex] = true
				queue = append(queue, nextVertex)
			}
		}
	}

	// If we exhaust the queue without finding the destination, return false
	return false, nil
}

func (algo *SVK) checkReachability(src string, dst string) (bool, error) {
	algo.updateSvkOptionally()
	svLabel := algo.SV

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
