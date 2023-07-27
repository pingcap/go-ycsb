// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/measurement"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

var KVReqGenChan chan *ycsb.KVRequest
var KVReqDoChan chan *ycsb.KVRequest

type worker struct {
	p               *properties.Properties
	workDB          ycsb.DB
	workload        ycsb.Workload
	doTransactions  bool
	doBatch         bool
	batchSize       int
	opCount         int64
	targetOpsPerMs  float64
	threadID        int
	threadIDStr     string
	targetOpsTickNs int64
	opsDone         int64
	overflowOpCount int64
}

func newWorker(p *properties.Properties, threadID int, threadCount int, workload ycsb.Workload, db ycsb.DB) *worker {
	w := new(worker)
	w.p = p
	w.doTransactions = p.GetBool(prop.DoTransactions, true)
	w.batchSize = p.GetInt(prop.BatchSize, prop.DefaultBatchSize)
	if w.batchSize > 1 {
		w.doBatch = true
	}
	w.threadID = threadID
	w.threadIDStr = fmt.Sprintf("tid%v", threadID)
	w.workload = workload
	w.workDB = db

	var totalOpCount int64
	if w.doTransactions {
		totalOpCount = p.GetInt64(prop.OperationCount, 0)
	} else {
		if _, ok := p.Get(prop.InsertCount); ok {
			totalOpCount = p.GetInt64(prop.InsertCount, 0)
		} else {
			totalOpCount = p.GetInt64(prop.RecordCount, 0)
		}
	}

	if totalOpCount < int64(threadCount) {
		fmt.Printf("totalOpCount(%s/%s/%s): %d should be bigger than threadCount: %d",
			prop.OperationCount,
			prop.InsertCount,
			prop.RecordCount,
			totalOpCount,
			threadCount)

		os.Exit(-1)
	}

	w.opCount = totalOpCount / int64(threadCount)

	targetPerThreadPerms := float64(-1)
	if v := p.GetInt64(prop.Target, 0); v > 0 {
		targetPerThread := float64(v) / float64(threadCount)
		targetPerThreadPerms = targetPerThread / 1000.0
	}

	if targetPerThreadPerms > 0 {
		w.targetOpsPerMs = targetPerThreadPerms
		w.targetOpsTickNs = int64(1000000.0 / w.targetOpsPerMs)
	}

	return w
}

func (w *worker) throttle(ctx context.Context, startTime time.Time) {
	if w.targetOpsPerMs <= 0 {
		return
	}

	d := time.Duration(w.opsDone * w.targetOpsTickNs)
	d = startTime.Add(d).Sub(time.Now())
	if d < 0 {
		return
	}
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func (w *worker) run(ctx context.Context) {
	// spread the thread operation out so they don't all hit the DB at the same time
	if w.targetOpsPerMs > 0.0 && w.targetOpsPerMs <= 1.0 {
		time.Sleep(time.Duration(rand.Int63n(w.targetOpsTickNs)))
	}

	startTime := time.Now()
	enableWaitingQueue := w.p.GetBool(prop.WaitingQueue, prop.WaitingQueueDefault)

	//========qps waiting queue===============
	if enableWaitingQueue {

		MaxQueueSize := w.p.GetInt(prop.WaitingQueueSize, prop.WaitingQueueSizeDefault)
		c := make(chan time.Time, MaxQueueSize) //max_size
		finish := make(chan struct{})

		go func() {
			for {
				var err error
				opsCount := 1
				start := <-c

				if start.Before(startTime) {
					break
				}
				if w.doTransactions {
					if w.doBatch {
						err = w.workload.DoBatchTransaction(ctx, w.batchSize, w.workDB)
						opsCount = w.batchSize
					} else {
						err = w.workload.DoTransaction(ctx, w.workDB)
					}
				} else {
					if w.doBatch {
						err = w.workload.DoBatchInsert(ctx, w.batchSize, w.workDB)
						opsCount = w.batchSize
					} else {
						err = w.workload.DoInsert(ctx, w.workDB)
					}
				}

				lan := time.Since(start)
				measurement.ThreadMeasure("AllWQ", lan)

				if err != nil && !w.p.GetBool(prop.Silence, prop.SilenceDefault) {
					fmt.Printf("operation err: %v\n", err)
				}

				if measurement.IsWarmUpFinished() {
					w.opsDone += int64(opsCount)
					w.throttle(ctx, startTime)
				}

				select {
				case <-ctx.Done():
					return
				default:
				}
			}
			finish <- struct{}{}
		}()

		threadCount := w.p.GetInt(prop.ThreadCount, 1)
		enableFluctLoad := w.p.GetBool(prop.FluctuateLoad, prop.FluctuateLoadDefault)
		fluctSineA := w.p.GetFloat64(prop.FluctuateSineA, prop.FluctuateSineADefault)
		fluctSineB := w.p.GetFloat64(prop.FluctuateSineB, prop.FluctuateSineBDefault)
		fluctSineC := w.p.GetFloat64(prop.FluctuateSineC, prop.FluctuateSineCDefault)
		fluctSineD := w.p.GetFloat64(prop.FluctuateSineD, prop.FluctuateSineDDefault)

		opCount := int64(0)
		timeUnit := 1.0
		for opCount < w.opCount {
			curQps := 1000.0 * timeUnit
			curTime := time.Now()
			if enableFluctLoad {
				curSec := curTime.Sub(startTime).Seconds()
				curQps = (fluctSineA*math.Sin(fluctSineB*curSec+fluctSineC) + fluctSineD) / float64(threadCount) * timeUnit
				if curQps < 1 {
					curQps = 1
				}
			}
			// if w.threadID == 0 {
			// 	fmt.Println("curQPs", curQps*float64(threadCount))
			// }

			tmpOp := int64(0)
			wakeInterval := int64(timeUnit * float64(time.Second.Nanoseconds()) / curQps)
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Duration(wakeInterval)):
					if len(c) == MaxQueueSize { // overflow, drop op
						w.overflowOpCount += 1
						// opCount += 1
						// tmpOp += 1
					} else {
						c <- time.Now()
						opCount += 1
						tmpOp += 1
					}
				}
				if time.Since(curTime).Seconds() > timeUnit {
					break
				}
			}
			// fmt.Println(w.threadIDStr, "sendOps", tmpOp)

			// tmpOp := int64(0)
			// for {
			// 	now := time.Now()
			// 	if len(c) == MaxQueueSize { // overflow, drop op
			// 		w.overflowOpCount += 1
			// 		// opCount += 1
			// 		// tmpOp += 1
			// 	} else {
			// 		c <- now
			// 		opCount += 1
			// 		tmpOp += 1
			// 	}

			// 	if now.Sub(curTime).Seconds() > timeUnit || tmpOp > int64(curQps) {
			// 		break
			// 	}
			// }
			// leftDur := time.Until(curTime.Add(time.Duration(int64(timeUnit) * time.Second.Nanoseconds())))
			// select {
			// case <-ctx.Done():
			// case <-time.After(leftDur):
			// }

		}

		c <- startTime.Add(-1000)
		<-finish
	} else { // normal logic
		useTraceReplay := w.p.GetBool(prop.TraceReplay, prop.TraceReplayDefault)

		for w.opCount == 0 || w.opsDone < w.opCount {
			var err error
			opsCount := 1
			start := time.Now()
			if w.doTransactions {
				if w.doBatch {
					err = w.workload.DoBatchTransaction(ctx, w.batchSize, w.workDB)
					opsCount = w.batchSize
				} else {
					err = w.workload.DoTransaction(ctx, w.workDB)
				}
			} else {
				if w.doBatch {
					err = w.workload.DoBatchInsert(ctx, w.batchSize, w.workDB)
					opsCount = w.batchSize
				} else {
					err = w.workload.DoInsert(ctx, w.workDB)
				}
			}
			lan := time.Since(start)

			if !useTraceReplay {
				measurement.ThreadMeasure(w.threadIDStr, lan)
			}

			if err != nil && !w.p.GetBool(prop.Silence, prop.SilenceDefault) {
				fmt.Printf("operation err: %v\n", err)
			}

			if measurement.IsWarmUpFinished() {
				w.opsDone += int64(opsCount)
				w.throttle(ctx, startTime)
			}

			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}
}

func KVReplay(ctx context.Context, p *properties.Properties, threadID int, threadCount int, workload ycsb.Workload, db ycsb.DB) {
	enableFluctLoad := p.GetBool(prop.FluctuateLoad, prop.FluctuateLoadDefault)
	fluctSineA := p.GetFloat64(prop.FluctuateSineA, prop.FluctuateSineADefault)
	fluctSineB := p.GetFloat64(prop.FluctuateSineB, prop.FluctuateSineBDefault)
	fluctSineC := p.GetFloat64(prop.FluctuateSineC, prop.FluctuateSineCDefault)
	fluctSineD := p.GetFloat64(prop.FluctuateSineD, prop.FluctuateSineDDefault)
	startTime := time.Now()

	opCount := int64(0)
	timeUnit := 1.0
	for {
		curQps := 1000.0 * timeUnit
		curTime := time.Now()
		if enableFluctLoad {
			curSec := curTime.Sub(startTime).Seconds()
			curQps = (fluctSineA*math.Sin(fluctSineB*curSec+fluctSineC) + fluctSineD) / float64(threadCount) * timeUnit
			//curQps = ((fluctSineA*math.Sin(fluctSineB*curSec+fluctSineC) + fluctSineD) + (0.3 * fluctSineA * math.Sin(40*fluctSineB*curSec+fluctSineC))) / float64(threadCount) * timeUnit
			if curQps < 1 {
				curQps = 1
			}
		}

		tmpOp := int64(0)
		wakeInterval := int64(timeUnit * float64(time.Second.Nanoseconds()) / curQps)
		for {
			kvReq := <-KVReqGenChan
			time.Sleep(time.Duration(wakeInterval))
			kvReq.StartTime = time.Now()
			if len(KVReqDoChan) >= 1000000-1 {
				continue
			}
			KVReqDoChan <- kvReq
			opCount += 1
			tmpOp += 1
			if time.Since(curTime).Seconds() > timeUnit {
				break
			}
		}
	}
}

func KVApply(ctx context.Context, p *properties.Properties, threadID int, threadCount int, workload ycsb.Workload, db ycsb.DB) {
	ctx = workload.InitThread(ctx, threadID, threadCount)
	ctx = db.InitThread(ctx, threadID, threadCount)
	for {
		kvReq := <-KVReqDoChan
		workload.ProcessKVRequest(ctx, db, kvReq)
		lan := time.Since(kvReq.StartTime)
		measurement.ThreadMeasure("AllWQ", lan)
	}
}

// Client is a struct which is used the run workload to a specific DB.
type Client struct {
	p        *properties.Properties
	workload ycsb.Workload
	db       ycsb.DB
}

// NewClient returns a client with the given workload and DB.
// The workload and db can't be nil.
func NewClient(p *properties.Properties, workload ycsb.Workload, db ycsb.DB) *Client {
	return &Client{p: p, workload: workload, db: db}
}

// Run runs the workload to the target DB, and blocks until all workers end.
func (c *Client) Run(ctx context.Context) {
	var wg sync.WaitGroup
	threadCount := c.p.GetInt(prop.ThreadCount, 1)

	enableWaitingQueue := c.p.GetBool(prop.WaitingQueue, prop.WaitingQueueDefault)
	useTraceReplay := c.p.GetBool(prop.TraceReplay, prop.TraceReplayDefault)
	KVReqGenChan = make(chan *ycsb.KVRequest, 1000000)
	KVReqDoChan = make(chan *ycsb.KVRequest, 1000000)

	if useTraceReplay {
		threadCount = 16
	}

	wg.Add(threadCount)
	measureCtx, measureCancel := context.WithCancel(ctx)
	measureCh := make(chan struct{}, 1)
	go func() {
		defer func() {
			measureCh <- struct{}{}
		}()
		// load stage no need to warm up
		if c.p.GetBool(prop.DoTransactions, true) {
			dur := c.p.GetInt64(prop.WarmUpTime, 0)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(dur) * time.Second):
			}
		}
		// finish warming up
		measurement.EnableWarmUp(false)

		dur := c.p.GetInt64(prop.LogInterval, 10)
		t := time.NewTicker(time.Duration(dur) * time.Second)
		defer t.Stop()

		for {
			select {
			case <-t.C:
				if enableWaitingQueue || useTraceReplay {
					measurement.ThreadOutput()
				}
				measurement.Output()
			case <-measureCtx.Done():
				measurement.ThreadOutput()
				return
			}
		}
	}()

	mu := sync.Mutex{}
	totalOverflowOps := int64(0)

	for i := 0; i < threadCount; i++ {
		go func(threadId int) {
			defer wg.Done()

			w := newWorker(c.p, threadId, threadCount, c.workload, c.db)
			ctx := c.workload.InitThread(ctx, threadId, threadCount)
			ctx = c.db.InitThread(ctx, threadId, threadCount)
			w.run(ctx)
			c.db.CleanupThread(ctx)
			c.workload.CleanupThread(ctx)

			mu.Lock()
			totalOverflowOps += w.overflowOpCount
			mu.Unlock()
		}(i)
	}

	applyCount := c.p.GetInt(prop.ApplyThreadCount, 512)
	for i := 0; i < applyCount; i++ {
		go KVApply(ctx, c.p, i, applyCount, c.workload, c.db)
	}

	replayCount := c.p.GetInt(prop.ReplayThreadCount, 512)
	for i := 0; i < replayCount; i++ {
		go KVReplay(ctx, c.p, i, replayCount, c.workload, c.db)
	}

	wg.Wait()

	fmt.Println("Total Overflow Op Count:", totalOverflowOps)

	if !c.p.GetBool(prop.DoTransactions, true) {
		// when loading is finished, try to analyze table if possible.
		if analyzeDB, ok := c.db.(ycsb.AnalyzeDB); ok {
			analyzeDB.Analyze(ctx, c.p.GetString(prop.TableName, prop.TableNameDefault))
		}
	}
	measureCancel()
	<-measureCh
}
