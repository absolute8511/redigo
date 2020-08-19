// Copyright 2012 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package redis

import (
	"bytes"
	"log"
	"sync/atomic"
	"time"

	"github.com/absolute8511/redigo/internal"
)

const (
	sleepBetweenRetry = time.Microsecond * 200
)

type QueuePool struct {

	// Dial is an application supplied function for creating and configuring a
	// connection.
	//
	// The connection returned from Dial must not be in a special state
	// (subscribed to pubsub channel, transaction started, ...).
	Dial func() (Conn, error)

	// TestOnBorrow is an optional application supplied function for checking
	// the health of an idle connection before the connection is used again by
	// the application. Argument t is the time that the connection was returned
	// to the pool. If the function returns an error, then the connection is
	// closed.
	TestOnBorrow func(c Conn, t time.Time) error

	// Maximum number of idle connections in the pool.
	MaxIdle int

	// Maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	MaxActive int32

	// Close connections after remaining idle for this duration. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	IdleTimeout time.Duration

	pool       *connectionQueue
	closed     int32
	connCnt    int32
	waitList   chan struct{}
	waitingCnt int64
}

// NewPool creates a new pool.
//
// Deprecated: Initialize the Pool directory as shown in the example.
func NewQueuePool(newFn func() (Conn, error), maxIdle int, maxActive int) *QueuePool {
	return &QueuePool{
		Dial:      newFn,
		MaxIdle:   maxIdle,
		MaxActive: int32(maxActive),
		pool:      newConnectionQueue(maxActive),
		waitList:  make(chan struct{}, maxActive+1),
	}
}

func (p *QueuePool) IsActive() bool {
	return atomic.LoadInt32(&p.closed) == 0
}

func (p *QueuePool) GetUntil(timeout time.Duration, hint int) (Conn, error) {
	if timeout == 0 {
		timeout = time.Second
	}
	return p.getWithMaxRetry(timeout, hint, int64(timeout/sleepBetweenRetry))
}

// Get will only retry 3 times to avoid too much cpu cost
func (p *QueuePool) Get(timeout time.Duration, hint int) (Conn, error) {
	return p.getWithMaxRetry(timeout, hint, 3)
}

func (p *QueuePool) getWithMaxRetry(timeout time.Duration, hint int, maxRetry int64) (Conn, error) {
	var conn Conn
	var err error
	if !p.IsActive() {
		return nil, ErrPoolExhausted
	}
	realTo := timeout
	if timeout == 0 {
		realTo = time.Second
	}
	deadline := time.Now().Add(realTo)
	retry := 0
	var to *time.Timer
	defer func() {
		if to != nil {
			to.Stop()
		}
	}()

	firstWait := false
	if atomic.LoadInt64(&p.waitingCnt) > 1 {
		// since others is waiting, we need wait next retry
		firstWait = true
		err = ErrPoolExhausted
	}
CL:
	retry++
	if !firstWait {
		// try to acquire a connection; if the connection pool is empty, retry until
		// timeout occures. If no timeout is set, will retry indefinitely.
		// TODO: use pid for hint to reduce contention
		conn, err = p.getConnectionWithHint(hint)
	}
	firstWait = false
	if err != nil {
		//log.Printf("get conn failed: %v", err)
		if err == ErrPoolExhausted && p.IsActive() && time.Now().Before(deadline) {
			if int64(retry) >= maxRetry {
				// avoid too much retry while pool exhausted (it will cost too much cpu)
				// TODO; maybe use sync.Cond
				return nil, err
			}
			if to == nil {
				to = time.NewTimer(realTo)
			}
			atomic.AddInt64(&p.waitingCnt, 1)
			timeouted := false
			select {
			case <-to.C:
				timeouted = true
			case <-p.waitList:
			}
			atomic.AddInt64(&p.waitingCnt, -1)
			if timeouted {
				return nil, err
			}
			// since the conn may be grabbed by others in high concurrency, so avoid retry too quickly,
			// give the scheduler time to breath; affects latency minimally, but throughput drastically
			time.Sleep(sleepBetweenRetry)
			goto CL
		}

		return nil, err
	}
	return conn, nil
}

// getConnectionWithHint gets a connection to the node.
// If no pooled connection is available, a new connection will be created.
// This method does not include logic to retry in case the connection pool is empty
func (p *QueuePool) getConnectionWithHint(hint int) (Conn, error) {
	// try to get a valid connection from the connection pool
	var err error
	var conn *queuePooledConnection
	for t := p.pool.Poll(hint); t != nil; t = p.pool.Poll(hint) {
		conn = t
		if p.TestOnBorrow != nil {
			if p.TestOnBorrow(conn, conn.getLastUsedTime()) != nil {
				conn.realClose()
				conn = nil
				continue
			}
		}
		if conn.IsConnected() && conn.Err() == nil {
			break
		}
		conn.realClose()
		conn = nil
	}

	if conn == nil {
		cc := atomic.AddInt32(&p.connCnt, 1)
		// if connection count is limited and enough connections are already created, don't create a new one
		if cc > atomic.LoadInt32(&p.MaxActive) {
			atomic.AddInt32(&p.connCnt, -1)
			return nil, ErrPoolExhausted
		}

		var c Conn
		if c, err = p.Dial(); err != nil {
			atomic.AddInt32(&p.connCnt, -1)
			return nil, err
		}
		conn = &queuePooledConnection{c: c, p: p, hint: hint}
	}

	conn.SetIdleTimeout(p.IdleTimeout)
	conn.Refresh()

	return conn, nil
}

// PutConnection puts back a connection to the pool.
// If connection pool is full, the connection will be
// closed and discarded.
func (p *QueuePool) putConnectionWithHint(conn Conn, hint int) {
	pc, ok := conn.(*queuePooledConnection)
	if !ok {
		log.Printf("pool conn type invalid: %v", conn)
		return
	}
	pc.Refresh()
	if !p.IsActive() || !p.pool.Offer(pc, hint) {
		pc.realClose()
	}
	// if returned to pool, we notify waiting to retry get from pool
	// if closed, we notify waiting client to retry create a new connection to pool
	if atomic.LoadInt64(&p.waitingCnt) > 0 {
		select {
		case p.waitList <- struct{}{}:
		default:
		}
	}
}

// PutConnection puts back a connection to the pool.
// If connection pool is full, the connection will be
// closed and discarded.
func (p *QueuePool) PutConnection(conn Conn, hint int) {
	// TODO: use pid for hint to reduce contention
	p.putConnectionWithHint(conn, hint)
}

func (p *QueuePool) SetMaxActive(c int32) {
	atomic.StoreInt32(&p.MaxActive, c)
}

func (p *QueuePool) Count() int {
	active := atomic.LoadInt32(&p.connCnt)
	return int(active)
}

// Close releases the resources used by the pool.
func (p *QueuePool) Close() error {
	atomic.StoreInt32(&p.closed, 1)
	p.closeConnections()
	return nil
}

func (p *QueuePool) closeConnections() {
	for conn := p.pool.Poll(0); conn != nil; conn = p.pool.Poll(0) {
		conn.realClose()
	}
}

// refresh will drop idle connections and left the max idle (the left idle connection will ping for active)
func (p *QueuePool) Refresh() {
	//log.Printf("refreshing at : %v", time.Now())
	p.pool.DropIdle(p.MaxIdle)
}

func (p *QueuePool) put(c Conn, forceClose bool, hint int) error {
	if forceClose || c.Err() != nil || !p.IsActive() {
		pc := c.(*queuePooledConnection)
		pc.realClose()
		if atomic.LoadInt64(&p.waitingCnt) > 0 {
			//notify waiting to retry created one in pool
			select {
			case p.waitList <- struct{}{}:
			default:
			}
		}
		return nil
	}
	p.PutConnection(c, hint)
	return nil
}

type queuePooledConnection struct {
	p            *QueuePool
	c            Conn
	state        int
	idleTimeout  time.Duration
	idleDeadline time.Time
	hint         int
}

func (pc *queuePooledConnection) RemoteAddrStr() string {
	if pc.c != nil {
		return pc.c.RemoteAddrStr()
	}
	return ""
}

func (pc *queuePooledConnection) IsConnected() bool {
	if pc.c == nil {
		return false
	}
	_, ok := pc.c.(errorConnection)
	return !ok
}

// just close conn without put to pool
func (pc *queuePooledConnection) realClose() error {
	c := pc.c
	if _, ok := c.(errorConnection); ok {
		return nil
	}
	pc.c = errorConnection{errConnClosed}
	atomic.AddInt32(&pc.p.connCnt, -1)
	return c.Close()
}

// put conn to pool
func (pc *queuePooledConnection) Close() error {
	c := pc.c
	if _, ok := c.(errorConnection); ok {
		return nil
	}

	if pc.state&internal.MultiState != 0 {
		c.Send("DISCARD")
		pc.state &^= (internal.MultiState | internal.WatchState)
	} else if pc.state&internal.WatchState != 0 {
		c.Send("UNWATCH")
		pc.state &^= internal.WatchState
	}
	if pc.state&internal.SubscribeState != 0 {
		c.Send("UNSUBSCRIBE")
		c.Send("PUNSUBSCRIBE")
		// To detect the end of the message stream, ask the server to echo
		// a sentinel value and read until we see that value.
		sentinelOnce.Do(initSentinel)
		c.Send("ECHO", sentinel)
		c.Flush()
		for {
			p, err := c.Receive()
			if err != nil {
				break
			}
			if p, ok := p.([]byte); ok && bytes.Equal(p, sentinel) {
				pc.state &^= internal.SubscribeState
				break
			}
		}
	}
	c.Do("")
	pc.p.put(pc, pc.state != 0, pc.hint)
	return nil
}

func (pc *queuePooledConnection) Err() error {
	return pc.c.Err()
}

func (pc *queuePooledConnection) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	ci := internal.LookupCommandInfo(commandName)
	pc.state = (pc.state | ci.Set) &^ ci.Clear
	return pc.c.Do(commandName, args...)
}

func (pc *queuePooledConnection) Send(commandName string, args ...interface{}) error {
	ci := internal.LookupCommandInfo(commandName)
	pc.state = (pc.state | ci.Set) &^ ci.Clear
	return pc.c.Send(commandName, args...)
}

func (pc *queuePooledConnection) Flush() error {
	return pc.c.Flush()
}

func (pc *queuePooledConnection) Receive() (reply interface{}, err error) {
	return pc.c.Receive()
}

// setIdleTimeout sets the idle timeout for the connection.
func (pc *queuePooledConnection) SetIdleTimeout(timeout time.Duration) {
	pc.idleTimeout = timeout
}

// isIdle returns true if the connection has reached the idle deadline.
func (pc *queuePooledConnection) IsIdle() bool {
	return pc.idleTimeout > 0 && !time.Now().Before(pc.idleDeadline)
}

// refresh extends the idle deadline of the connection.
func (pc *queuePooledConnection) Refresh() {
	pc.idleDeadline = time.Now().Add(pc.idleTimeout)
}

func (pc *queuePooledConnection) Ping() {
	if pc.c != nil {
		pc.c.Do("PING")
	}
}

func (pc *queuePooledConnection) getLastUsedTime() time.Time {
	return pc.idleDeadline.Add(-1 * pc.idleTimeout)
}
