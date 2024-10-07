package indexer

import (
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"sync"
)

type WriteQueue struct {
	Size      uint
	Available uint
	Items     []*Operation
	mutex     sync.Mutex
	channel   chan *Operation
}

func NewWriteQueue(size uint) *WriteQueue {
	operations := make([]*Operation, 0, size)
	channel := make(chan *Operation)
	wq := &WriteQueue{
		size,
		size,
		operations,
		sync.Mutex{},
		channel,
	}
	go wq.SerialInserter()
	return wq
}

func (wq *WriteQueue) Add(op *Operation) {
	wq.mutex.Lock()
	defer wq.mutex.Unlock()
	wq.Items = append(wq.Items, op)
	wq.Available--
}

func (wq *WriteQueue) SerialInserter() {
	for operation := range wq.channel {
		wq.Add(operation)
	}
}

func (wq *WriteQueue) InsertToQueue(opType string, tuple *corev1.RelationTuple) {
	op := &Operation{
		OpType:      opType,
		Source:      makeNodeNameFromObjectRelationPair(tuple.ResourceAndRelation),
		Destination: makeNodeNameFromObjectRelationPair(tuple.Subject),
		Relation:    "",
	}
	wq.channel <- op
}

func (wq *WriteQueue) DrainQueue() []*Operation {
	wq.mutex.Lock()
	defer wq.mutex.Unlock()
	ops := wq.Items
	wq.Items = make([]*Operation, 0, wq.Size)
	wq.Available = 0
	return ops
}
