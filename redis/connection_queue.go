// Copyright 2013-2017 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redis

import (
	"runtime"
	"sync"
)

// singleConnectionQueue is a non-blocking FIFO queue.
// If the queue is empty, nil is returned.
// if the queue is full, offer will return false
type singleConnectionQueue struct {
	head, tail uint32
	data       []*queuePooledConnection
	size       uint32
	wrapped    bool
	mutex      sync.Mutex
}

// NewQueue creates a new queue with initial size.
func newSingleConnectionQueue(size int) *singleConnectionQueue {
	if size <= 0 {
		panic("Queue size cannot be less than 1")
	}

	return &singleConnectionQueue{
		wrapped: false,
		data:    make([]*queuePooledConnection, uint32(size)),
		size:    uint32(size),
	}
}

// Offer adds an item to the queue unless the queue is full.
// In case the queue is full, the item will not be added to the queue
// and false will be returned
func (q *singleConnectionQueue) Offer(conn *queuePooledConnection) bool {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	// make sure queue is not full
	if q.tail == q.head && q.wrapped {
		return false
	}

	if q.head+1 == q.size {
		q.wrapped = true
	}

	q.head = (q.head + 1) % q.size
	q.data[q.head] = conn
	return true
}

// Poll removes and returns an item from the queue.
// If the queue is empty, nil will be returned.
func (q *singleConnectionQueue) Poll() (res *queuePooledConnection) {
	q.mutex.Lock()

	// if queue is not empty
	if q.wrapped || (q.tail != q.head) {
		if q.tail+1 == q.size {
			q.wrapped = false
		}
		q.tail = (q.tail + 1) % q.size
		res = q.data[q.tail]
	}

	q.mutex.Unlock()
	return res
}

// singleConnectionQueue is a non-blocking FIFO queue.
// If the queue is empty, nil is returned.
// if the queue is full, offer will return false
type connectionQueue struct {
	queues []singleConnectionQueue
}

func newConnectionQueue(size int) *connectionQueue {
	queueCount := runtime.NumCPU() / 2
	if size <= 0 {
		size = 8
	}
	if queueCount > size {
		queueCount = size
	}
	if queueCount <= 0 {
		queueCount = 1
	}
	// avoid too much count which will cause the cpu high while pool is full
	if queueCount > 4 {
		queueCount = 4
	}
	// will be >= 1
	perQueueSize := size / queueCount

	queues := make([]singleConnectionQueue, queueCount)
	for i := range queues {
		queues[i] = *newSingleConnectionQueue(perQueueSize)
	}

	// add a queue for the remainder
	if (perQueueSize*queueCount)-size > 0 {
		queues = append(queues, *newSingleConnectionQueue(size - queueCount*perQueueSize))
	}

	return &connectionQueue{
		queues: queues,
	}
}

// Offer adds an item to the queue unless the queue is full.
// In case the queue is full, the item will not be added to the queue
// and false will be returned
func (q *connectionQueue) Offer(conn *queuePooledConnection, hint int) bool {
	idx := hint % len(q.queues)
	end := idx + len(q.queues)
	for i := idx; i < end; i++ {
		if success := q.queues[i%len(q.queues)].Offer(conn); success {
			return true
		}
	}
	return false
}

// Poll removes and returns an item from the queue.
// If the queue is empty, nil will be returned.
func (q *connectionQueue) Poll(hint int) (res *queuePooledConnection) {
	// fmt.Println(int(hint) % len(q.queues))

	idx := hint

	end := idx + len(q.queues)
	for i := idx; i < end; i++ {
		if conn := q.queues[i%len(q.queues)].Poll(); conn != nil {
			return conn
		}
	}
	return nil
}

// DropIdle closes all idle connections.
func (q *connectionQueue) DropIdle(keep int) {
L:
	for i := 0; i < len(q.queues); i++ {
		for conn := q.queues[i].Poll(); conn != nil; conn = q.queues[i].Poll() {
			refreshed := false
			//log.Printf("conn deadline: %v", conn.idleDeadline)
			if keep > 0 && conn.IsConnected() && conn.IsIdle() {
				keep--
				conn.Ping()
				conn.Refresh()
				refreshed = true
				//log.Printf("idle conn refreshed: %v", conn.idleDeadline)
			}
			if conn.IsConnected() && !conn.IsIdle() {
				// put it back: this connection is the oldest, and is still fresh
				// so the ones after it are likely also fresh
				if !q.queues[i].Offer(conn) {
					conn.realClose()
				}
				if !refreshed {
					//log.Printf("check next idle conn pool %v: %v", i, conn.idleDeadline)
					continue L
				} else {
					//log.Printf("continue check current idle conn pool:%v", i, conn.idleDeadline)
					continue
				}
			}

			//log.Printf("idle conn closed: %v", conn.idleDeadline)
			conn.realClose()
		}
	}
}
