package core

import (
	"sync"
	"time"
)

type ObserveActionType = byte

const (
	ObserveActionPut ObserveActionType = iota
	ObserveActionDelete
)

type Event struct {
	Action ObserveActionType
	Key    []byte
	Value  []byte
	TxId   uint64
}

type Observer struct {
	queue allocQueue
	mu    sync.RWMutex
}

func NewObserver(capacity uint64) *Observer {
	return &Observer{
		queue: allocQueue{
			Events:   make([]*Event, capacity),
			Capacity: capacity,
		},
	}
}

func (o *Observer) putEvent(e *Event) {
	o.mu.Lock()
	o.queue.push(e)
	if o.queue.isFull() {
		o.queue.frontTakeAStep()
	}
	o.mu.Unlock()
}

func (o *Observer) getEvent() *Event {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.queue.isEmpty() {
		return nil
	}
	return o.queue.pop()
}

func (o *Observer) sendEvent(c chan *Event) {
	for {
		e := o.getEvent()
		if e == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		c <- e
	}
}

type allocQueue struct {
	Events   []*Event
	Capacity uint64
	Front    uint64
	Back     uint64
}

func (aq *allocQueue) push(e *Event) {
	aq.Events[aq.Back] = e
	aq.Back = (aq.Back + 1) % aq.Capacity
}

func (aq *allocQueue) pop() *Event {
	aqe := aq.Events[aq.Front]
	aq.frontTakeAStep()
	return aqe
}

func (aq *allocQueue) isFull() bool {
	return (aq.Back+1)%aq.Capacity == aq.Front
}

func (aq *allocQueue) isEmpty() bool {
	return aq.Back == aq.Front
}

func (aq *allocQueue) frontTakeAStep() {
	aq.Front = (aq.Front + 1) % aq.Capacity
}
