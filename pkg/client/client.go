package client

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type worker struct {
	p               *properties.Properties
	workDB          ycsb.DB
	workload        ycsb.Workload
	doTransactions  bool
	opCount         int64
	targetOpsPerMs  float64
	threadID        int
	targetOpsTickNs int64
	opsDone         int64
}

func newWorker(p *properties.Properties, threadID int, threadCount int, workload ycsb.Workload, db ycsb.DB) *worker {
	w := new(worker)
	w.p = p
	w.doTransactions = p.GetBool(prop.DoTransactions, true)
	w.threadID = threadID
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

func (w *worker) Run(ctx context.Context) {
	// spread the thread operation out so they don't all hit the DB at the same time
	if w.targetOpsPerMs > 0.0 && w.targetOpsPerMs <= 1.0 {
		time.Sleep(time.Duration(rand.Int63n(w.targetOpsTickNs)))
	}

	startTime := time.Now()

	for w.opCount == 0 || w.opsDone < w.opCount {
		var err error
		if w.doTransactions {
			err = w.workload.DoTransaction(ctx, w.workDB)
		} else {
			err = w.workload.DoInsert(ctx, w.workDB)
		}

		if err != nil {
			break
		}

		w.opsDone++
		w.throttle(ctx, startTime)

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

type Client struct {
	p        *properties.Properties
	wg       sync.WaitGroup
	workload ycsb.Workload
	db       ycsb.DB
}

// NewClient returns a client with the given workload and db.
// The workload and db can't be nil.
func NewClient(p *properties.Properties, workload ycsb.Workload, db ycsb.DB) *Client {
	return &Client{p: p, workload: workload, db: db}
}

func (c *Client) Run(ctx context.Context) {
	threadCount := c.p.GetInt(prop.ThreadCount, 100)

	c.wg.Add(threadCount)
	for i := 0; i < threadCount; i++ {
		go func(threadId int) {
			defer c.wg.Done()

			w := newWorker(c.p, threadId, threadCount, c.workload, c.db)
			ctx := c.workload.InitThread(ctx, threadId, threadCount)
			ctx = c.db.InitThread(ctx, threadId, threadCount)
			w.Run(ctx)
			c.db.CleanupThread(ctx)
			c.workload.CleanupThread(ctx)
		}(i)
	}
}

func (c *Client) Wait() {
	c.wg.Wait()
}
