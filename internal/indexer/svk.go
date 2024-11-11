package indexer

import (
	"fmt"
	"github.com/acmpesuecc/Onyx"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/hmdsefi/gograph"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
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
	Graph            *Onyx.Graph
	ReverseGraph     *Onyx.Graph
	SV               string
	RPairMutex       sync.RWMutex
	RPair            *RPair
	numReads         int
	blueQueue        *WriteQueue
	greenQueue       *WriteQueue
	CurQueueLock     sync.RWMutex
	CurrentQueue     *WriteQueue
	lastUpdated      time.Time
	lastUpdatedMu    sync.Mutex
	svkCheckCounter  *prometheus.CounterVec
	insertDurationMS prometheus.Summary
	promRegistry     *prometheus.Registry
}

type Operation struct {
	OpType      string // todo: make enum
	Source      string
	Destination string
	Relation    string
}

const opsThreshold = 50
const timeThresholdMillis = 10 * time.Second //500 * time.Millisecond

func (algo *SVK) applyWrites() {
	if !algo.lastUpdatedMu.TryLock() {
		return
	}
	defer algo.lastUpdatedMu.Unlock()
	start_time := time.Now()

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

	time_taken := algo.lastUpdated.Sub(start_time).Nanoseconds()
	algo.insertDurationMS.Observe(float64(time_taken))
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
	//prometheus setup
	algo.promRegistry = prometheus.NewRegistry()
	algo.svkCheckCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "svk_check_count",
			Help: "Number of insert operations per approach",
		},
		[]string{"approach"})
	algo.insertDurationMS = prometheus.NewSummary(prometheus.SummaryOpts{Name: "insert_duration_ns", Help: "Time taken to insert the write queue"})
	algo.promRegistry.MustRegister(algo.svkCheckCounter, algo.insertDurationMS)
	http.Handle("/metrics", promhttp.HandlerFor(algo.promRegistry, promhttp.HandlerOpts{}))
	go func() {
		fmt.Println("SVK Prometheus metrics available at :9093/metrics")
		http.ListenAndServe(":9093", nil)
	}()
	//-------------------

	if graph == nil {
		graph, err := Onyx.NewGraph(BADGER_GRAPH_PATH, false || IN_MEMORY_GLOBAL)
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
	algo.ReverseGraph, err = Onyx.NewGraph(BADGER_REV_GRAPH_PATH, false || IN_MEMORY_GLOBAL)
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
	fmt.Println("[Pre-Init] ", algo.SV, " chosen as SV")
	return nil
}

func (algo *SVK) recompute() {
	copyOfRpair := RPair{R_Plus: make(map[string]bool), R_Minus: make(map[string]bool)}
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

	//for k, v := range pair.R_Plus {
	//	fmt.Println("[R+] ", k, ": ", v)
	//}
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

	//fmt.Println("========Printing R Minus=========")
	//for k, v := range pair.R_Minus {
	//	fmt.Println("[R-] ", k, ": ", v)
	//}
	return nil
}

func (algo *SVK) insertEdge(src string, dst string) error {
	algo.updateSvkOptionally()

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
	txn := graph.DB.NewTransaction(false)
	defer txn.Discard()

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
			err := txn.Commit()
			if err != nil {
				return false, err
			}

			return true, nil
		}

		neighbors, err := graph.GetEdges(currentVertex, txn)
		if err != nil {
			log.Print(err.Error())
			panic(err)
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

	err := txn.Commit()
	if err != nil {
		return false, err
	}

	// If we exhaust the queue without finding the destination, return false
	return false, nil
}

func (algo *SVK) checkReachability(src string, dst string) (isReachable bool, resolved bool, err error) {
	algo.updateSvkOptionally()
	svLabel := algo.SV

	if !algo.RPairMutex.TryRLock() {
		algo.svkCheckCounter.WithLabelValues("LOCK-FAIL-BYPASS").Inc()
		//return false, false, errors.New("[CheckReachability][Unresoled] failed to get RLock, Fallback to SpiceDb")
		return false, false, nil
	}
	defer algo.RPairMutex.RUnlock()

	//if src is support vertex
	if svLabel == src {
		algo.svkCheckCounter.WithLabelValues("SRC").Inc()
		fmt.Println("[CheckReachability][Resolved] Src vertex is SV")
		return algo.RPair.R_Plus[dst], true, nil
	}

	//if dest is support vertex
	if svLabel == dst {
		algo.svkCheckCounter.WithLabelValues("DST").Inc()
		fmt.Println("[CheckReachability][Resolved] Dst vertex is SV")
		return algo.RPair.R_Minus[dst], true, nil
	}

	//try to apply O1
	if algo.RPair.R_Minus[src] == true && algo.RPair.R_Plus[dst] == true {
		algo.svkCheckCounter.WithLabelValues("O1").Inc()
		fmt.Println("[CheckReachability][Resolved] Using O1")
		return true, true, nil
	}

	//try to apply O2
	if algo.RPair.R_Plus[src] == true && algo.RPair.R_Plus[dst] == false {
		algo.svkCheckCounter.WithLabelValues("O2").Inc()
		fmt.Println("[CheckReachability][Resolved] Using O2")
		return false, true, nil
	}

	//try to apply O3
	if algo.RPair.R_Minus[src] == false && algo.RPair.R_Minus[dst] == true {
		algo.svkCheckCounter.WithLabelValues("O3").Inc()
		fmt.Println("[CheckReachability][Resolved] Using O3")
		return false, true, nil
	}

	//if all else fails, fallback to BFS
	fmt.Println("[CheckReachability][Resolved] Fallback to BFS")
	algo.svkCheckCounter.WithLabelValues("BFS").Inc()
	if !DO_BFS {
		return false, false, nil
	}

	bfs, err := directedBFS(algo.Graph, src, dst)
	if err != nil {
		return false, false, err
	}

	if bfs == true {
		return true, true, nil
	}
	algo.numReads++
	return false, true, nil
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
