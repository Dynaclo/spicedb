package indexer

import (
	"github.com/Dynaclo/Onyx"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"sync"
	"time"
)

var Index SVK
var graph *Onyx.Graph

func AddEdge(tuple *corev1.RelationTuple) {
	if graph == nil {
		var err error
		graph, err = Onyx.NewGraph("/tmp/tmpgraph", false)
		if err != nil {
			panic(err)
		}
	}
	src := makeNodeNameFromObjectRelationPair(tuple.ResourceAndRelation)
	dest := makeNodeNameFromObjectRelationPair(tuple.Subject)
	err := graph.AddEdge(src, dest, nil)
	if err != nil {
		panic(err)
	}
}

func NewIndex() {
	blueQueue := NewWriteQueue(100)
	sv := SVK{numReads: -1, blueQueue: blueQueue, greenQueue: NewWriteQueue(100), CurQueueLock: sync.RWMutex{}, RPair: &RPair{}, lastUpdated: time.Now(), lastUpdatedMu: sync.Mutex{}}
	sv.CurrentQueue = blueQueue
	sv.NewIndex(graph)
	Index = sv
}

func (algo *SVK) InsertEdge(tuple *corev1.RelationTuple) error {
	src := makeNodeNameFromObjectRelationPair(tuple.ResourceAndRelation)
	dest := makeNodeNameFromObjectRelationPair(tuple.Subject)
	return algo.insertEdge(src, dest)
}

func (algo *SVK) Check(req *v1.CheckPermissionRequest) (v1.CheckPermissionResponse_Permissionship, error) {
	src := req.Resource.ObjectType + ":" + req.Resource.ObjectId
	dst := req.Subject.Object.ObjectType + ":" + req.Subject.Object.ObjectId
	hasPermission, err := algo.checkReachability(src, dst)
	if err != nil {
		return v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION, err
	}

	if hasPermission {
		return v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, nil
	} else {
		return v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION, nil
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
