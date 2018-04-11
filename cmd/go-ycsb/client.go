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

package main

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/go-ycsb/pkg/measurement"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/spf13/cobra"

	"github.com/magiconair/properties"
)

type worker struct {
	p               *properties.Properties
	doTransactions  bool
	opCount         int64
	targetOpsPerMs  float64
	threadID        int
	targetOpsTickNs int64
	opsDone         int64
}

func newWorker(p *properties.Properties, threadID int, threadCount int) *worker {
	w := new(worker)
	w.p = p
	w.doTransactions = p.GetBool(prop.DoTransactions, true)
	w.threadID = threadID

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
			err = globalWorkload.DoTransaction(ctx, globalDB)
		} else {
			err = globalWorkload.DoInsert(ctx, globalDB)
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

type client struct {
	p *properties.Properties
}

func (c *client) Run() {
	start := time.Now()
	threadCount := c.p.GetInt(prop.ThreadCount, 100)

	var wg sync.WaitGroup
	wg.Add(threadCount)
	for i := 0; i < threadCount; i++ {
		go func(threadId int) {
			defer wg.Done()

			w := newWorker(c.p, threadId, threadCount)
			ctx := globalWorkload.InitThread(globalContext, threadId, threadCount)
			ctx = globalDB.InitThread(ctx, threadId, threadCount)
			w.Run(ctx)
			globalDB.CleanupThread(ctx)
			globalWorkload.CleanupThread(ctx)
		}(i)
	}

	measureCtx, measureCancel := context.WithCancel(globalContext)
	go func() {
		dur := c.p.GetInt64("measurement.interval", 10)
		t := time.NewTicker(time.Duration(dur) * time.Second)
		defer t.Stop()

		for {
			select {
			case <-t.C:
				measurement.Output()
			case <-measureCtx.Done():
				return
			}
		}
	}()

	wg.Wait()

	measureCancel()

	fmt.Printf("Run finished, takes %s\n", time.Now().Sub(start))
	measurement.Output()
}

func runClientCommandFunc(cmd *cobra.Command, args []string, doTransactions bool) {
	dbName := args[0]

	initialGlobal(dbName, func() {
		doTransFlag := "true"
		if !doTransactions {
			doTransFlag = "false"
		}
		globalProps.Set(prop.DoTransactions, doTransFlag)

		if threadsArg > 1 {
			// We set the threadArg via command line.
			globalProps.Set(prop.ThreadCount, strconv.Itoa(threadsArg))
		}

		if targetArg > 0 {
			globalProps.Set(prop.Target, strconv.Itoa(targetArg))
		}
	})

	c := &client{
		p: globalProps,
	}
	c.Run()
}

func runLoadCommandFunc(cmd *cobra.Command, args []string) {
	runClientCommandFunc(cmd, args, false)
}

func runTransCommandFunc(cmd *cobra.Command, args []string) {
	runClientCommandFunc(cmd, args, true)
}

var (
	threadsArg int
	targetArg  int
)

func initClientCommand(m *cobra.Command) {
	m.Flags().StringVarP(&propertyFile, "property_file", "P", "", "Spefify a property file")
	m.Flags().StringSliceVarP(&propertyValues, "prop", "p", nil, "Specify a property value with name=value")
	m.Flags().StringVar(&tableName, "table", "", "Use the table name instead of the default \""+prop.TableNameDefault+"\"")
	m.Flags().IntVar(&threadsArg, "threads", 1, "execute using n threads - can also be specified as the \"threadcount\" property")
	m.Flags().IntVar(&targetArg, "target", 0, "attempt to do n operations per second (default: unlimited) - can also be specified as the \"target\" property")
}

func newLoadCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "load db",
		Short: "YCSB load benchmark",
		Args:  cobra.MinimumNArgs(1),
		Run:   runLoadCommandFunc,
	}

	initClientCommand(m)
	return m
}

func newRunCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "run db",
		Short: "YCSB run benchmark",
		Args:  cobra.MinimumNArgs(1),
		Run:   runTransCommandFunc,
	}

	initClientCommand(m)
	return m
}
