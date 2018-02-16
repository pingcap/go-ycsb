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
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/magiconair/properties"

	// Register workload

	"fmt"

	"github.com/pingcap/go-ycsb/pkg/measurement"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	_ "github.com/pingcap/go-ycsb/pkg/workload"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/spf13/cobra"

	// Register basic database
	_ "github.com/pingcap/go-ycsb/db/basic"
	// Register MySQL database
	_ "github.com/pingcap/go-ycsb/db/mysql"
)

var (
	propertyFile   string
	propertyValues []string
	dbName         string
	tableName      string

	globalContext context.Context
	globalCancel  context.CancelFunc

	globalDB       ycsb.DB
	globalWorkload ycsb.Workload
	globalProps    *properties.Properties
)

func initialGlobal(dbName string, onProperties func()) {
	globalProps = properties.NewProperties()
	if len(propertyFile) > 0 {
		globalProps = properties.MustLoadFile(propertyFile, properties.UTF8)
	}

	for _, prop := range propertyValues {
		seps := strings.SplitN(prop, "=", 2)
		globalProps.Set(seps[0], seps[1])
	}

	if onProperties != nil {
		onProperties()
	}

	addr := globalProps.GetString(prop.DebugPprof, prop.DebugPprofDefault)
	go func() {
		http.ListenAndServe(addr, nil)
	}()

	measurement.InitMeasure(globalProps)

	if len(tableName) == 0 {
		tableName = globalProps.GetString(prop.TableName, prop.TableNameDefault)
	}

	workloadName := globalProps.GetString(prop.Workload, "core")
	workloadCreator := ycsb.GetWorkloadCreator(workloadName)

	var err error
	if globalWorkload, err = workloadCreator.Create(globalProps); err != nil {
		util.Fatalf("create workload %s failed %v", workloadName, err)
	}

	dbCreator := ycsb.GetDBCreator(dbName)
	if globalDB, err = dbCreator.Create(globalProps); err != nil {
		util.Fatalf("create db %s failed %v", dbName, err)
	}

	globalDB = dbWrapper{globalDB}
}

func main() {
	globalContext, globalCancel = context.WithCancel(context.Background())

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		fmt.Printf("\nGot signal [%v] to exit.\n", sig)
		globalCancel()
	}()

	rootCmd := &cobra.Command{
		Use:   "go-ycsb",
		Short: "Go YCSB",
	}

	rootCmd.AddCommand(
		newShellCommand(),
		newLoadCommand(),
		newRunCommand(),
	)

	cobra.EnablePrefixMatching = true

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(rootCmd.UsageString())
	}

	globalCancel()
	if globalDB != nil {
		globalDB.Close()
	}

	if globalWorkload != nil {
		globalWorkload.Close()
	}
}
