// Copyright 2021 PingCAP, Inc.
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

package workload

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type workload struct {
	clientCount int
	clientID    int
}

type threadedState struct {
	r           *rand.Rand
	threadID    int // Real thread ID in all clients
	threadCount int // Real thread count for all clients
}

// Init implements the Workload Close interface.
func (w *workload) Init(db ycsb.DB) error {
	// TODO: create real tables here.
	sqlDB := db.ToSqlDB()
	_, err := sqlDB.Exec("create table if not exists X (a int primary key, b int)")
	return err
}

// Load implements the Workload Close interface.
func (w *workload) Load(ctx context.Context, db ycsb.DB, totalCount int64) error {
	state := ctx.Value("state").(*threadedState)
	if totalCount%int64(state.threadCount) != 0 {
		return errors.New("recordcount % threadcount should be 0")
	}

	sqlDB := db.ToSqlDB()
	count := totalCount / int64(state.threadCount)
	startID := count * int64(state.threadID)

	// Adjust startID to continue at break point.
	var startIDContinue int64 = 0
	q := fmt.Sprintf("select max(id) from X where id between {} and {}", startID, startID+count-1)
	if err := sqlDB.QueryRow(q).Scan(&startIDContinue); err == nil {
		startID = startIDContinue
	} else if err != sql.ErrNoRows {
		return err
	}

	// TODO: load data here.
	for id := startID; id < startID+count; id++ {

	}
	return nil
}

// Close implements the Workload Close interface.
func (w *workload) Close() error {
	return nil
}

// InitThread implements the Workload InitThread interface.
func (w *workload) InitThread(ctx context.Context, threadID int, threadCount int, db ycsb.DB) context.Context {
	seed := rand.NewSource(time.Now().UnixNano() + int64(threadID*10000000))
	state := &threadedState{
		r:           rand.New(seed),
		threadID:    w.clientID*threadCount + threadID,
		threadCount: w.clientCount * threadCount,
	}
	return context.WithValue(ctx, "state", state)
}

// CleanupThread implements the Workload CleanupThread interface.
func (w *workload) CleanupThread(_ context.Context) {
}

// DoInsert implements the Workload DoInsert interface.
func (w *workload) DoInsert(ctx context.Context, db ycsb.DB) error {
	return nil
}

// DoBatchInsert implements the Workload DoBatchInsert interface.
func (w *workload) DoBatchInsert(ctx context.Context, batchSize int, db ycsb.DB) error {
	return nil
}

// DoTransaction implements the Workload DoTransaction interface.
func (w *workload) DoTransaction(ctx context.Context, db ycsb.DB) error {
	// TODO: implement it.
	return nil
}

// DoBatchTransaction implements the Workload DoTransaction interface.
func (w *workload) DoBatchTransaction(ctx context.Context, batchSize int, db ycsb.DB) error {
	// TODO: implement it.
	return nil
}

type creator struct{}

func (creator) Create(p *properties.Properties) (ycsb.Workload, error) {
	clientCount := p.MustGetInt("clientcount")
	clientID := p.MustGetInt("clientid")
	return &workload{
		clientCount: clientCount,
		clientID:    clientID,
	}, nil
}

func init() {
	ycsb.RegisterWorkloadCreator("wallet", creator{})
}
