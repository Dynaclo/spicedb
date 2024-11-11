package indexer

import (
	"fmt"
	"github.com/acmpesuecc/Onyx"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"os"
	"sync"
	"time"
)

var Index SVK
var graph *Onyx.Graph
var IN_MEMORY_GLOBAL bool = false
var DO_BFS bool = true
var BADGER_GRAPH_PATH string = "./onyx-graph"
var BADGER_REV_GRAPH_PATH string = "./onyx-graph-rev"

func deleteBadgerGraphsIfExist() {
	folders := []string{BADGER_GRAPH_PATH, BADGER_REV_GRAPH_PATH}

	for _, folderPath := range folders {
		if _, err := os.Stat(folderPath); !os.IsNotExist(err) {
			if err := os.RemoveAll(folderPath); err != nil {
				fmt.Println("Error deleting folder:", folderPath, err)
			} else {
				fmt.Println("Folder deleted successfully:", folderPath)
			}
		}
	}
}

func AddEdge(tuple *corev1.RelationTuple) {
	if graph == nil {
		deleteBadgerGraphsIfExist()

		var err error
		graph, err = Onyx.NewGraph(BADGER_GRAPH_PATH, false || IN_MEMORY_GLOBAL)
		if err != nil {
			panic(err)
		}
	}
	src := makeNodeNameFromObjectRelationPair(tuple.ResourceAndRelation)
	dest := makeNodeNameFromObjectRelationPair(tuple.Subject)
	fmt.Printf("[Pre-Init] Added edge %s->%s\n", src, dest)
	err := graph.AddEdge(src, dest, nil)
	if err != nil {
		panic(err)
	}
}

func NewIndex() {
	blueQueue := NewWriteQueue(100)
	sv := SVK{numReads: -1, blueQueue: blueQueue, greenQueue: NewWriteQueue(100), CurQueueLock: sync.RWMutex{}, RPair: &RPair{}, lastUpdated: time.Now(), lastUpdatedMu: sync.Mutex{}}
	sv.CurrentQueue = blueQueue
	err := sv.NewIndex(graph)
	if err != nil {
		panic(err)
	}
	Index = sv
}

func (algo *SVK) InsertEdge(tuple *corev1.RelationTuple) error {
	src := makeNodeNameFromObjectRelationPair(tuple.ResourceAndRelation)
	dest := makeNodeNameFromObjectRelationPair(tuple.Subject)
	return algo.insertEdge(src, dest)
}

func (algo *SVK) Check(req *v1.CheckPermissionRequest) (v1.CheckPermissionResponse_Permissionship, bool, error) {
	src := req.Resource.ObjectType + ":" + req.Resource.ObjectId
	dst := req.Subject.Object.ObjectType + ":" + req.Subject.Object.ObjectId
	hasPermission, resolved, err := algo.checkReachability(src, dst)
	if err != nil {
		return v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION, false, err
	}
	if !resolved {
		return v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION, false, nil
	}

	//if resolved
	if hasPermission {
		return v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, true, nil
	} else {
		return v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION, true, nil
	}
}

//func (algo *SVK) DumpGraph() {
//	err := generateDotFile(algo.Graph, "graph.dot")
//	if err != nil {
//		panic(err)
//	}
//}

func makeNodeNameFromObjectRelationPair(relation *corev1.ObjectAndRelation) string {
	return relation.Namespace + ":" + relation.ObjectId
}
