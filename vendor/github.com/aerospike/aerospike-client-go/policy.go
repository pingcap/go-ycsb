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
	"fmt"
	"time"
)

// Policy Interface
type Policy interface {
	// Retrieves BasePolicy
	GetBasePolicy() *BasePolicy
}

// BasePolicy encapsulates parameters for transaction policy attributes
// used in all database operation calls.
type BasePolicy struct {
	Policy

	// Priority of request relative to other transactions.
	// Currently, only used for scans.
	Priority Priority //= Priority.DEFAULT;

	// How replicas should be consulted in a read operation to provide the desired
	// consistency guarantee. Default to allowing one replica to be used in the
	// read operation.
	ConsistencyLevel ConsistencyLevel //= CONSISTENCY_ONE

	// Timeout specifies total transaction timeout.
	//
	// The Timeout is tracked on the client and also sent to the server along
	// with the transaction in the wire protocol. The client will most likely
	// timeout first, but the server has the capability to Timeout the transaction.
	//
	// If Timeout is not zero and Timeout is reached before the transaction
	// completes, the transaction will abort with Timeout error.
	//
	// If Timeout is zero, there will be no time limit and the transaction will retry
	// on network timeouts/errors until MaxRetries is exceeded. If MaxRetries is exceeded, the
	// transaction also aborts with Timeout error.
	//
	// Default: 0 (no time limit and rely on MaxRetries).
	Timeout time.Duration

	// SocketTimeout determines network timeout for each attempt.
	//
	// If SocketTimeout is not zero and SocketTimeout is reached before an attempt completes,
	// the Timeout above is checked. If Timeout is not exceeded, the transaction
	// is retried. If both SocketTimeout and Timeout are non-zero, SocketTimeout must be less
	// than or equal to Timeout.
	//
	// If SocketTimeout is zero, there will be no time limit per attempt. If the transaction
	// fails on a network error, the Timeout still applies.
	//
	// Default: 0 (no SocketTimeout for each attempt).
	SocketTimeout time.Duration

	// MaxRetries determines the maximum number of retries before aborting the current transaction.
	// The initial attempt is not counted as a retry.
	//
	// If MaxRetries is exceeded, the transaction will abort with an error.
	//
	// WARNING: Database writes that are not idempotent (such as AddOp)
	// should not be retried because the write operation may be performed
	// multiple times if the client timed out previous transaction attempts.
	// It's important to use a distinct WritePolicy for non-idempotent
	// writes which sets maxRetries = 0;
	//
	// Default for read: 2 (initial attempt + 2 retries = 3 attempts)
	//
	// Default for write/query/scan: 0 (no retries)
	MaxRetries int //= 2;

	// SleepBetweenRtries determines the duration to sleep between retries.  Enter zero to skip sleep.
	// This field is ignored when maxRetries is zero.
	// This field is also ignored in async mode.
	//
	// The sleep only occurs on connection errors and server timeouts
	// which suggest a node is down and the cluster is reforming.
	// The sleep does not occur when the client's socketTimeout expires.
	//
	// Reads do not have to sleep when a node goes down because the cluster
	// does not shut out reads during cluster reformation.  The default for
	// reads is zero.
	//
	// The default for writes is also zero because writes are not retried by default.
	// Writes need to wait for the cluster to reform when a node goes down.
	// Immediate write retries on node failure have been shown to consistently
	// result in errors.  If maxRetries is greater than zero on a write, then
	// sleepBetweenRetries should be set high enough to allow the cluster to
	// reform (>= 500ms).
	SleepBetweenRetries time.Duration //= 1ms;

	// SleepMultiplier specifies the multiplying factor to be used for exponential backoff during retries.
	// Default to (1.0); Only values greater than 1 are valid.
	SleepMultiplier float64 //= 1.0;

	// SendKey determines to whether send user defined key in addition to hash digest on both reads and writes.
	// If the key is sent on a write, the key will be stored with the record on
	// the server.
	// The default is to not send the user defined key.
	SendKey bool // = false

	// Force reads to be linearized for server namespaces that support CP mode.
	// The default is false.
	LinearizeRead bool

	// ReplicaPolicy determines the node to send the read commands containing the key's partition replica type.
	// Write commands are not affected by this setting, because all writes are directed
	// to the node containing the key's master partition.
	// Batch, scan and query are also not affected by replica algorithms.
	// Default to sending read commands to the node containing the key's master partition.
	ReplicaPolicy ReplicaPolicy
}

// NewPolicy generates a new BasePolicy instance with default values.
func NewPolicy() *BasePolicy {
	return &BasePolicy{
		Priority:            DEFAULT,
		ConsistencyLevel:    CONSISTENCY_ONE,
		Timeout:             0 * time.Millisecond,
		SocketTimeout:       100 * time.Millisecond,
		MaxRetries:          2,
		SleepBetweenRetries: 1 * time.Millisecond,
		SleepMultiplier:     1.0,
		ReplicaPolicy:       SEQUENCE,
		SendKey:             false,
		LinearizeRead:       false,
	}
}

var _ Policy = &BasePolicy{}

// GetBasePolicy returns embedded BasePolicy in all types that embed this struct.
func (p *BasePolicy) GetBasePolicy() *BasePolicy { return p }

// socketTimeout validates and then calculates the timeout to be used for the socket
// based on Timeout and SocketTimeout values.
func (p *BasePolicy) socketTimeout() (time.Duration, error) {
	if p.Timeout == 0 {
		if p.SocketTimeout > 0 {
			return p.SocketTimeout, nil
		}
		return 0, nil
	} else if p.Timeout > 0 {
		if p.SocketTimeout == 0 {
			return p.Timeout, nil
		} else if p.SocketTimeout > 0 {
			if p.SocketTimeout > p.Timeout {
				return 0, fmt.Errorf("Socket timeout %v is longer than the total transaction Timeout %v", p.SocketTimeout, p.Timeout)
			}
			return p.SocketTimeout, nil
		}
	}
	return 0, fmt.Errorf("Invalid Socket timeout %v and/or total transaction Timeout %v", p.SocketTimeout, p.Timeout)
}
