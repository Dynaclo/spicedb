package indexer

import corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"

type WriteQueue struct {
	Size      uint
	Available uint
	Items     []*Operation
	channel   chan *Operation
}

func NewWriteQueue(size uint) *WriteQueue {
	operations := make([]*Operation, 0, size)
	channel := make(chan *Operation)
	wq := &WriteQueue{
		size,
		size,
		operations,
		channel,
	}
	go wq.SerialInserter()
	return wq
}

func (wq *WriteQueue) SerialInserter() {
	for operation := range wq.channel {
		wq.Items = append(wq.Items, operation)
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
