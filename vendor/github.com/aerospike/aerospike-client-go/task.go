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

package aerospike

import (
	"time"

	. "github.com/aerospike/aerospike-client-go/types"
	. "github.com/aerospike/aerospike-client-go/types/atomic"
)

// Task interface defines methods for asynchronous tasks.
type Task interface {
	IsDone() (bool, error)

	onComplete(ifc Task) chan error
	OnComplete() chan error
}

// baseTask is used to poll for server task completion.
type baseTask struct {
	retries AtomicInt
	cluster *Cluster
}

// newTask initializes task with fields needed to query server nodes.
func newTask(cluster *Cluster) *baseTask {
	return &baseTask{
		cluster: cluster,
	}
}

// Wait for asynchronous task to complete using default sleep interval.
func (btsk *baseTask) onComplete(ifc Task) chan error {
	ch := make(chan error, 1)

	// goroutine will loop every <interval> until IsDone() returns true or error
	const interval = 1 * time.Second
	go func() {
		// always close the channel on return
		defer close(ch)

		for {
			select {
			case <-time.After(interval):
				done, err := ifc.IsDone()
				btsk.retries.IncrementAndGet()
				if err != nil {
					if _, ok := err.(AerospikeError); ok && err.(AerospikeError).ResultCode() == TIMEOUT {
						err.(AerospikeError).MarkInDoubt()
					}
					ch <- err
					return
				} else if done {
					ch <- nil
					return
				}
			} // select
		} // for
	}()

	return ch
}
