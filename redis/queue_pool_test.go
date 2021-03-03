// Copyright 2011 Gary Burd
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

package redis_test

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/absolute8511/redigo/redis"
)

type qpoolTestConn struct {
	d   *qpoolDialer
	err error
	redis.Conn
}

func (c *qpoolTestConn) Close() error {
	c.d.mu.Lock()
	c.d.open -= 1
	c.d.mu.Unlock()
	return c.Conn.Close()
}

func (c *qpoolTestConn) Err() error { return c.err }

func (c *qpoolTestConn) Do(commandName string, args ...interface{}) (interface{}, error) {
	if commandName == "ERR" {
		c.err = args[0].(error)
		commandName = "PING"
	}
	if commandName != "" {
		c.d.commands = append(c.d.commands, commandName)
	}
	return c.Conn.Do(commandName, args...)
}

func (c *qpoolTestConn) Send(commandName string, args ...interface{}) error {
	c.d.commands = append(c.d.commands, commandName)
	return c.Conn.Send(commandName, args...)
}

type qpoolDialer struct {
	mu       sync.Mutex
	t        *testing.T
	dialed   int
	open     int
	commands []string
	dialErr  error
	dialWait int
}

func (d *qpoolDialer) dial() (redis.Conn, error) {
	d.mu.Lock()
	d.dialed += 1
	dialErr := d.dialErr
	wait := d.dialWait
	d.mu.Unlock()
	if wait > 0 {
		time.Sleep(time.Second * time.Duration(wait))
	}
	if dialErr != nil {
		return nil, dialErr
	}
	c, err := redis.DialDefaultServer()
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	d.open += 1
	d.mu.Unlock()
	return &qpoolTestConn{d: d, Conn: c}, nil
}

func (d *qpoolDialer) check(message string, p *redis.QueuePool, dialed, open int) {
	d.mu.Lock()
	if d.dialed != dialed {
		d.t.Errorf("%s: dialed=%d, want %d", message, d.dialed, dialed)
	}
	if d.open != open {
		d.t.Errorf("%s: open=%d, want %d", message, d.open, open)
	}

	if active := p.Count(); active != open {
		d.t.Errorf("%s: active=%d, want %d", message, active, open)
	}
	d.mu.Unlock()
}

func TestQPoolReuse(t *testing.T) {
	d := qpoolDialer{t: t}
	p := redis.NewQueuePool(d.dial, 2, 10)

	for i := 0; i < 10; i++ {
		c1, err := p.Get(0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if c1.RemoteAddrStr()[:10] != "127.0.0.1:" {
			t.Fatalf("remote is unexpected: %v", c1.RemoteAddrStr())
		}
		c1.Do("PING")
		c2, _ := p.Get(0, 0)
		c2.Do("PING")
		c1.Close()
		c2.Close()
	}

	d.check("before close", p, 2, 2)
	p.Close()
	d.check("after close", p, 2, 0)
}

func TestQPoolMaxIdle(t *testing.T) {
	d := qpoolDialer{t: t}
	p := redis.NewQueuePool(d.dial, 1, 20)
	p.IdleTimeout = time.Millisecond * 100

	defer p.Close()

	for i := 0; i < 10; i++ {
		c1, _ := p.Get(0, 0)
		c1.Do("PING")
		c2, _ := p.Get(0, 0)
		c2.Do("PING")
		c3, _ := p.Get(0, 0)
		c3.Do("PING")
		c1.Close()
		c2.Close()
		c3.Close()
	}
	d.check("before close", p, 3, 3)
	time.Sleep(time.Millisecond * 200)
	p.Refresh()
	d.check("after refresh", p, 3, p.MaxIdle)
}

func TestQPoolError(t *testing.T) {
	d := qpoolDialer{t: t}
	p := redis.NewQueuePool(d.dial, 2, 20)

	defer p.Close()

	c, _ := p.Get(0, 0)
	c.Do("ERR", io.EOF)
	if c.Err() == nil {
		t.Errorf("expected c.Err() != nil")
	}
	c.Close()

	c, _ = p.Get(0, 0)
	c.Do("ERR", io.EOF)
	c.Close()

	d.check(".", p, 2, 0)
}

func TestQPoolClose(t *testing.T) {
	d := qpoolDialer{t: t}
	p := redis.NewQueuePool(d.dial, 2, 20)

	defer p.Close()

	c1, _ := p.Get(0, 0)
	c1.Do("PING")
	c2, _ := p.Get(0, 0)
	c2.Do("PING")
	c3, _ := p.Get(0, 0)
	c3.Do("PING")

	c1.Close()
	c2.Close()

	d.check("after conn close", p, 3, 3)
	p.Close()

	c3.Close()
	c1, _ = p.Get(0, 0)
	if c1 != nil {
		t.Errorf("expected error after pool closed")
	}
}

func TestQPoolBorrowCheck(t *testing.T) {
	d := qpoolDialer{t: t}
	p := redis.NewQueuePool(d.dial, 2, 20)
	p.TestOnBorrow = func(redis.Conn, time.Time) error { return redis.Error("BLAH") }
	defer p.Close()

	for i := 0; i < 10; i++ {
		c, _ := p.Get(0, 0)
		c.Do("PING")
		c.Close()
	}
	d.check("1", p, 10, 1)
}

func TestQPoolBorrowTime(t *testing.T) {
	d := qpoolDialer{t: t}
	p := redis.NewQueuePool(d.dial, 2, 10)
	p.TestOnBorrow = func(r redis.Conn, last time.Time) error {
		t.Logf("last used %v, now %v", last, time.Now())
		if time.Since(last) < time.Second {
			t.Errorf("borrow time not ok: %v, %v", last, time.Now())
		}
		return nil
	}
	defer p.Close()

	c1, err := p.Get(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	c1.Do("PING")
	c1.Close()
	d.check("1", p, 1, 1)
	time.Sleep(time.Second)

	c1, err = p.Get(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	c1.Do("PING")
	c1.Close()
	d.check("2", p, 1, 1)
}

func TestQPoolMaxActive(t *testing.T) {
	d := qpoolDialer{t: t}
	p := redis.NewQueuePool(d.dial, 2, 2)
	defer p.Close()

	c1, _ := p.Get(0, 0)
	c1.Do("PING")
	c2, _ := p.Get(0, 0)
	c2.Do("PING")

	d.check("1", p, 2, 2)

	c3, err := p.Get(0, 0)
	if err != redis.ErrPoolExhausted {
		t.Errorf("expected pool exhausted")
	}

	d.check("2", p, 2, 2)
	c2.Close()
	d.check("3", p, 2, 2)

	c3, _ = p.Get(0, 0)
	if _, err := c3.Do("PING"); err != nil {
		t.Errorf("expected good channel, err=%v", err)
	}
	c3.Close()

	d.check("4", p, 2, 2)
}

func TestQPoolWaitTimeout(t *testing.T) {
	d := qpoolDialer{t: t}
	p := redis.NewQueuePool(d.dial, 1, 1)
	defer p.Close()

	c1, _ := p.Get(0, 0)
	c1.Do("PING")
	c2, err := p.Get(time.Second, 0)
	if err != redis.ErrPoolExhausted {
		t.Errorf("expected pool exhausted")
	}
	d.check("1", p, 1, 1)
	c1.Close()

	c2, err = p.Get(time.Second, 0)
	if err != nil {
		t.Errorf("should success get conn")
	}
	d.check("2", p, 1, 1)
	c2.Close()
	d.check("3", p, 1, 1)
}
func TestQPoolMonitorCleanup(t *testing.T) {
	d := qpoolDialer{t: t}
	p := redis.NewQueuePool(d.dial, 2, 2)
	defer p.Close()

	c, _ := p.Get(0, 0)
	c.Send("MONITOR")
	c.Close()

	d.check("", p, 1, 0)
}

func startQPoolGoroutines(p *redis.QueuePool, cmd string, args ...interface{}) chan error {
	errs := make(chan error, 10)
	for i := 0; i < cap(errs); i++ {
		go func() {
			c, err := p.Get(time.Second*2, 0)
			if err != nil {
				errs <- err
			} else {
				_, err := c.Do(cmd, args...)
				c.Close()
				errs <- err
			}
		}()
	}

	// Wait for goroutines to block.
	time.Sleep(time.Second / 4)

	return errs
}

func TestWaitQPool(t *testing.T) {
	d := qpoolDialer{t: t}
	p := redis.NewQueuePool(d.dial, 1, 1)
	defer p.Close()

	c, _ := p.Get(0, 0)
	errs := startQPoolGoroutines(p, "PING")
	d.check("before close", p, 1, 1)
	c.Close()
	timeout := time.After(2 * time.Second)
	for i := 0; i < cap(errs); i++ {
		select {
		case err := <-errs:
			if err != nil {
				if err != redis.ErrPoolExhausted {
					t.Fatal(err)
				}
			}
		case <-timeout:
			t.Fatalf("timeout waiting for blocked goroutine %d", i)
		}
	}
	d.check("done", p, 1, 1)
}

func TestWaitQPoolClose(t *testing.T) {
	d := qpoolDialer{t: t}
	p := redis.NewQueuePool(d.dial, 1, 1)

	defer p.Close()

	c, _ := p.Get(0, 0)
	if _, err := c.Do("PING"); err != nil {
		t.Fatal(err)
	}
	errs := startQPoolGoroutines(p, "PING")
	d.check("before close", p, 1, 1)
	p.Close()
	timeout := time.After(2 * time.Second)
	for i := 0; i < cap(errs); i++ {
		select {
		case err := <-errs:
			switch err {
			case nil:
				t.Fatal("blocked goroutine did not get error")
			case redis.ErrPoolExhausted:
				t.Log("blocked goroutine got pool exhausted error")
			}
		case <-timeout:
			t.Fatal("timeout waiting for blocked goroutine")
		}
	}
	c.Close()
	d.check("done", p, 1, 0)
}

func TestWaitQPoolCommandError(t *testing.T) {
	testErr := errors.New("test")
	d := qpoolDialer{t: t}
	p := redis.NewQueuePool(d.dial, 1, 1)
	defer p.Close()

	c, _ := p.Get(0, 0)
	errs := startQPoolGoroutines(p, "ERR", testErr)
	d.check("before close", p, 1, 1)
	c.Close()
	timeout := time.After(2 * time.Second)
	for i := 0; i < cap(errs); i++ {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatal(err)
			}
		case <-timeout:
			t.Fatalf("timeout waiting for blocked goroutine %d", i)
		}
	}
	d.check("done", p, cap(errs), 0)
}

func TestWaitQPoolDialError(t *testing.T) {
	testErr := errors.New("test")
	d := qpoolDialer{t: t}
	p := redis.NewQueuePool(d.dial, 1, 1)
	defer p.Close()

	c, _ := p.Get(0, 0)
	errs := startQPoolGoroutines(p, "ERR", testErr)
	d.check("before close", p, 1, 1)

	d.dialErr = errors.New("dial")
	c.Close()

	nilCount := 0
	errCount := 0
	poolEmpty := 0
	timeout := time.After(2 * time.Second)
	for i := 0; i < cap(errs); i++ {
		select {
		case err := <-errs:
			switch err {
			case nil:
				nilCount++
			case d.dialErr:
				errCount++
			case redis.ErrPoolExhausted:
				poolEmpty++
			default:
				t.Fatalf("expected dial error or nil, got %v", err)
			}
		case <-timeout:
			t.Fatalf("timeout waiting for blocked goroutine %d", i)
		}
	}
	if nilCount != 1 {
		t.Errorf("expected one nil error, got %d", nilCount)
	}
	if errCount+poolEmpty != cap(errs)-1 {
		t.Errorf("expected %d dial errors, got %d", cap(errs)-1, errCount)
	}
	d.check("done", p, errCount+1, 0)
}

func TestWaitQPoolShouldFailfastToomuchWaiting(t *testing.T) {
	d := qpoolDialer{t: t}
	p := redis.NewQueuePool(d.dial, 1, 1)
	defer p.Close()
	d.check("before close", p, 0, 0)
	conn, err := p.Get(time.Second, 0)
	if err != nil {
		t.Errorf("failed: %v", err)
		return
	}
	conn.Close()

	conn, err = p.Get(time.Second/10, 0)
	if err != nil {
		t.Errorf("failed: %v", err)
		return
	}
	defer conn.Close()

	start := time.Now()
	_, err = p.Get(time.Second/2, 0)
	if err != redis.ErrPoolExhausted {
		t.Errorf("pool should exhausted: %v", err)
		return
	}
	wd := time.Since(start)
	if wd < time.Second/2 {
		t.Errorf("should wait exhausted enough: %s", wd)
		return
	}
	var wg sync.WaitGroup
	failFast := int32(0)
	errCnt := int32(0)
	for i := 0; i < 11; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			start := time.Now()
			_, err := p.Get(time.Second/2, 0)
			wd := time.Since(start)
			if wd < time.Millisecond {
				atomic.AddInt32(&failFast, 1)
			} else if wd < time.Second/2 {
				t.Errorf("should wait enough timeout: %s", wd)
			}
			if err != nil {
				atomic.AddInt32(&errCnt, 1)
			}
		}(i)
	}
	wg.Wait()
	t.Logf("stats: %v, %v, %v, %v", failFast, errCnt, p.Count(), p.WaitingCount())
	if failFast <= 0 {
		t.Errorf("should have fail fast error: %v", failFast)
	}
	if failFast >= 3 {
		t.Errorf("should not have  too much fail fast error: %v", failFast)
	}
	if errCnt < 10 {
		t.Errorf("should have enough error: %v", errCnt)
	}
}

func TestWaitQPoolShouldNotStarve(t *testing.T) {
	d := qpoolDialer{t: t}
	p := redis.NewQueuePool(d.dial, 1, 1)
	defer p.Close()

	d.check("before close", p, 0, 0)
	conn, err := p.Get(time.Second, 0)
	if err != nil {
		t.Errorf("failed: %v", err)
		return
	}
	conn.Close()
	time.Sleep(time.Second)

	errCnt := int32(0)
	errPoolExhaustedCnt := int32(0)
	successCnt := int32(0)
	done := make(chan struct{}, 1)
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			goSuccessCnt := 0
			goErrCnt := 0
			for {
				select {
				case <-done:
					t.Logf("g %v success: %v, err: %v", index, goSuccessCnt, goErrCnt)
					return
				default:
				}
				t.Logf("g %v begin get: %s", index, time.Now())
				conn, err := p.Get(time.Second/10, index)
				if err != nil {
					t.Logf("g %v get err: %s", index, time.Now())
					goErrCnt++
					if err == redis.ErrPoolExhausted {
						atomic.AddInt32(&errPoolExhaustedCnt, 1)
					}
					if atomic.AddInt32(&errCnt, 1) > 1 {
						break
					}
				} else {
					t.Logf("g %v done get conn: %s", index, time.Now())
					conn.Close()
					t.Logf("g %v release conn: %s", index, time.Now())
					atomic.AddInt32(&successCnt, 1)
					goSuccessCnt++
					time.Sleep(time.Microsecond * 10)
					if atomic.LoadInt32(&errCnt) > 1 {
						t.Logf("g %v quit since has err: %s", index, time.Now())
						break
					}
				}
			}
			t.Logf("g %v success: %v, err: %v", index, goSuccessCnt, goErrCnt)
		}(i)
	}
	// Wait for goroutines to block.
	time.Sleep(time.Second / 10)

	start := time.Now()
	for {
		if time.Since(start) > time.Second*5 {
			break
		}
		time.Sleep(time.Second / 4)
	}
	close(done)
	wg.Wait()

	t.Logf("stats: %v, %v, %v, pool: %v, %v", successCnt, errCnt, errPoolExhaustedCnt, p.Count(), p.WaitingCount())
	if p.Count() <= 0 {
		t.Errorf("should have pooled connections")
	}
	if successCnt <= 0 {
		t.Errorf("should have success get connections")
	}
	if errPoolExhaustedCnt > 1 {
		t.Errorf("should have no exhausted error")
	}
	if errCnt > 1 {
		t.Errorf("should have no error")
	}
	d.check("done", p, 1, 1)
}
func TestWaitQPoolDialTimeoutToomuchShouldNotHangWaiting(t *testing.T) {
	// test pool dail too long, and many client waiting on get connection since max active
	// and the dail failed should notify waiting client to retry recreate
	d := qpoolDialer{t: t, dialWait: 3}
	p := redis.NewQueuePool(d.dial, 1, 2)
	defer p.Close()

	d.dialErr = errors.New("dial")
	d.check("before close", p, 0, 0)

	errCnt := int32(0)
	errPoolExhaustedCnt := int32(0)
	successCnt := int32(0)
	done := make(chan struct{}, 1)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			time.Sleep(time.Millisecond * time.Duration(10*index))
			for {
				select {
				case <-done:
					return
				default:
				}
				conn, err := p.Get(time.Second/10, 0)
				if err != nil {
					atomic.AddInt32(&errCnt, 1)
					if err == redis.ErrPoolExhausted {
						atomic.AddInt32(&errPoolExhaustedCnt, 1)
					}
				} else {
					conn.Close()
					atomic.AddInt32(&successCnt, 1)
					time.Sleep(time.Millisecond)
				}
			}
		}(i)
	}
	// Wait for goroutines to block.
	time.Sleep(time.Second / 4)

	start := time.Now()
	for {
		if p.Count() == 0 && p.WaitingCount() > 5 {
			break
		}
		if time.Since(start) > time.Second*10 {
			break
		}
		time.Sleep(time.Second / 2)
	}
	t.Logf("stats: %v, %v, %v, pool: %v, %v", atomic.LoadInt32(&successCnt), atomic.LoadInt32(&errCnt),
		atomic.LoadInt32(&errPoolExhaustedCnt), p.Count(), p.WaitingCount())
	if atomic.LoadInt32(&successCnt) > 0 {
		t.Errorf("should not success dial")
	}
	// make dial become normal
	d.mu.Lock()
	d.dialErr = nil
	d.dialWait = 0
	d.mu.Unlock()
	// should have success for a while
	start = time.Now()
	for {
		if p.WaitingCount() == 0 {
			break
		}
		if time.Since(start) > time.Second*10 {
			t.Errorf("timeout wait the waiting done")
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
	close(done)
	wg.Wait()

	t.Logf("stats: %v, %v, %v, pool: %v, %v", successCnt, errCnt, errPoolExhaustedCnt, p.Count(), p.WaitingCount())
	if p.Count() <= 0 {
		t.Errorf("should have pooled connections")
	}
	if successCnt <= 0 {
		t.Errorf("should have success get connections")
	}
	if errPoolExhaustedCnt <= errCnt/2 {
		t.Errorf("should have enough exhausted error")
	}
	if errCnt-errPoolExhaustedCnt <= 2 {
		t.Errorf("should have enough non-exhausted error")
	}
	d.check("done", p, int(errCnt-errPoolExhaustedCnt+2), 2)
}

func BenchmarkQPoolGetWithLowIdle(b *testing.B) {
	b.StopTimer()
	p := redis.NewQueuePool(redis.DialDefaultServer, 2, 100)
	c, err := p.Get(0, 0)
	if err != nil {
		b.Fatal(err)
	}
	if err := c.Err(); err != nil {
		b.Fatal(err)
	}
	c.Close()
	defer p.Close()
	b.StartTimer()

	var wg sync.WaitGroup
	done := make(chan struct{})
	go func() {
		for {
			p.Refresh()
			select {
			case <-done:
				break
			default:
			}
			time.Sleep(time.Second * 3)
		}
	}()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				c, _ := p.Get(0, 0)
				c.Close()
			}
		}()
	}
	wg.Wait()
	close(done)
}

func BenchmarkQPoolGet(b *testing.B) {
	b.StopTimer()
	p := redis.NewQueuePool(redis.DialDefaultServer, 100, 100)
	c, err := p.Get(0, 0)
	if err != nil {
		b.Fatal(err)
	}
	if err := c.Err(); err != nil {
		b.Fatal(err)
	}
	c.Close()
	defer p.Close()
	b.StartTimer()
	done := make(chan struct{})
	go func() {
		for {
			p.Refresh()
			select {
			case <-done:
				break
			default:
			}
			time.Sleep(time.Second * 3)
		}
	}()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				c, _ := p.Get(0, 0)
				c.Close()
			}
		}()
	}
	wg.Wait()
	close(done)
}

func BenchmarkQPoolGetErr(b *testing.B) {
	b.StopTimer()
	p := redis.NewQueuePool(redis.DialDefaultServer, 2, 20)
	c, err := p.Get(0, 0)
	if err != nil {
		b.Fatal(err)
	}
	if err := c.Err(); err != nil {
		b.Fatal(err)
	}
	c.Close()
	defer p.Close()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c, err = p.Get(0, 0)
		if err != nil {
			b.Fatal(err)
		}
		c.Close()
	}
}

func BenchmarkQPoolGetPing(b *testing.B) {
	b.StopTimer()
	p := redis.NewQueuePool(redis.DialDefaultServer, 2, 20)
	c, err := p.Get(0, 0)
	if err != nil {
		b.Fatal(err)
	}
	c.Close()
	defer p.Close()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c, _ = p.Get(0, 0)
		if _, err := c.Do("PING"); err != nil {
			b.Fatal(err)
		}
		c.Close()
	}
}
