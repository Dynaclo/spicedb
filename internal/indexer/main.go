package indexer

import (
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/hmdsefi/gograph"
)

var Index SVK
var graph gograph.Graph[string]

func AddEdge(tuple *corev1.RelationTuple) {
	src := createVertexIfNeeded(tuple.ResourceAndRelation)
	dest := createVertexIfNeeded(tuple.Subject)
	_, err := graph.AddEdge(src, dest)
	if err != nil {
		panic(err)
	}
}

func StartIndexing() {
	graph = gograph.New[string](gograph.Directed())

}

func NewIndex() {
	sv := SVK{numReads: -1}
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

func (algo *SVK) DumpGraph() {
	err := generateDotFile(algo.Graph, "graph.dot")
	if err != nil {
		panic(err)
	}
}

func makeNodeNameFromObjectRelationPair(relation *corev1.ObjectAndRelation) string {
	return relation.Namespace + ":" + relation.ObjectId
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
