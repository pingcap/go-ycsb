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

// Modifyed by singheart
package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	"github.com/pingcap/go-ycsb/db/taas_proto"
	"github.com/pingcap/go-ycsb/db/taas_tikv"
	// "github.com/pingcap/go-ycsb/pkg/workload"
	"io/ioutil"
	"log"
	"strconv"
	"time"

	_ "github.com/pingcap/go-ycsb/db/taas_tikv"
	"github.com/pingcap/go-ycsb/pkg/client"
	"github.com/pingcap/go-ycsb/pkg/measurement"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/spf13/cobra"
)

func sendTxnToTaas(taasServerIp string, taasTxnCH chan taas_tikv.TaasTxn) {
	fmt.Println("连接Taas Send send")
	socket, _ := zmq.NewSocket(zmq.PUSH)
	err := socket.SetSndbuf(10000000)
	if err != nil {
		fmt.Println("连接Taas Send send 1")
		log.Fatal(err)
		return
	}
	err = socket.SetRcvbuf(10000000)
	if err != nil {
		fmt.Println("连接Taas Send send 2")
		log.Fatal(err)
		return
	}
	err = socket.SetSndhwm(10000000)
	if err != nil {
		fmt.Println("连接Taas Send send 3")
		log.Fatal(err)
		return
	}
	err = socket.SetRcvhwm(10000000)
	if err != nil {
		fmt.Println("连接Taas Send send 4")
		log.Fatal(err)
		return
	}
	err = socket.Connect("tcp://" + taasServerIp + ":5551")
	if err != nil {
		fmt.Println("taas.go 97")
		log.Fatal(err)
	}
	fmt.Println("连接Taas Send" + taasServerIp)
	for {
		value, ok := <-taasTxnCH
		if ok {
			_, err := socket.Send(string(value.GzipedTransaction), 0)
			if err != nil {
				return
			}
		} else {
			fmt.Println("taas.go 109")
			log.Fatal(ok)
		}
	}
}

func listenFromTaas(unPackCH chan string) {
	fmt.Println("连接Taas Send listen")
	socket, err := zmq.NewSocket(zmq.PULL)
	err = socket.SetSndbuf(10000000)
	if err != nil {
		fmt.Println("连接Taas Send listen 1")
		log.Fatal(err)
		return
	}
	err = socket.SetRcvbuf(10000000)
	if err != nil {
		fmt.Println("连接Taas Send listen 2")
		log.Fatal(err)
		return
	}
	err = socket.SetSndhwm(10000000)
	if err != nil {
		fmt.Println("连接Taas Send listen 3")
		log.Fatal(err)
		return
	}
	err = socket.SetRcvhwm(10000000)
	if err != nil {
		fmt.Println("连接Taas Send listen 4")
		log.Fatal(err)
		return
	}
	err = socket.Bind("tcp://*:5552")
	fmt.Println("连接Taas Listen")
	if err != nil {
		log.Fatal(err)
	}
	for {
		taasReply, err := socket.Recv(0)
		if err != nil {
			fmt.Println("taas.go 115")
			log.Fatal(err)
		}
		unPackCH <- taasReply
	}
}

func unGZipBytes(in []byte) []byte {

	reader, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		var out []byte
		return out
	}
	defer reader.Close()
	out, _ := ioutil.ReadAll(reader)
	return out

}

func unPack(unPackCH chan string, chanList []chan string) {
	//fmt.Println("连接Taas Send unpack")
	for {
		taasReply, ok := <-unPackCH
		if ok {
			UnGZipedReply := unGZipBytes([]byte(taasReply))
			testMessage := &taas_proto.Message{}
			err := proto.Unmarshal(UnGZipedReply, testMessage)
			if err != nil {
				fmt.Println("taas.go 142")
				log.Fatal(err)
			}
			replyMessage := testMessage.GetReplyTxnResultToClient()
			chanList[replyMessage.ClientTxnId%2048] <- replyMessage.GetTxnState().String()
		} else {
			fmt.Println("taas.go 148")
			log.Fatal(ok)
		}
	}
}

func runClientCommandFunc(cmd *cobra.Command, args []string, doTransactions bool, command string) {
	dbName := args[0]

	initialGlobal(dbName, func() {
		doTransFlag := "true"
		if !doTransactions {
			doTransFlag = "false"
		}
		globalProps.Set(prop.DoTransactions, doTransFlag)
		globalProps.Set(prop.Command, command)

		if cmd.Flags().Changed("threads") {
			// We set the threadArg via command line.
			globalProps.Set(prop.ThreadCount, strconv.Itoa(threadsArg))
		}

		if cmd.Flags().Changed("target") {
			globalProps.Set(prop.Target, strconv.Itoa(targetArg))
		}

		if cmd.Flags().Changed("interval") {
			globalProps.Set(prop.LogInterval, strconv.Itoa(reportInterval))
		}
	})

	fmt.Println("***************** properties *****************")
	for key, value := range globalProps.Map() {
		fmt.Printf("\"%s\"=\"%s\"\n", key, value)
	}
	fmt.Println("**********************************************")

	c := client.NewClient(globalProps, globalWorkload, globalDB)
	start := time.Now()
	c.Run(globalContext)
	fmt.Println("**********************************************")
	fmt.Printf("Run finished, takes %s\n", time.Now().Sub(start))
	measurement.Output()
	fmt.Printf("[Read] TotalOpNum %d, SuccessOpNum %d FailedOpNum %d SuccessRate %f\n",
		taas_tikv.TotalReadCounter, taas_tikv.SuccessReadCounter, taas_tikv.FailedReadCounter,
		float64(taas_tikv.SuccessReadCounter)/float64(taas_tikv.TotalReadCounter))
	fmt.Printf("[Update] TotalOpNum %d, SuccessOpNum %d FailedOpNum %d SuccessRate %f\n",
		taas_tikv.TotalUpdateCounter, taas_tikv.SuccessUpdateCounter, taas_tikv.FailedUpdateounter,
		float64(taas_tikv.SuccessUpdateCounter)/float64(taas_tikv.TotalUpdateCounter))
	fmt.Printf("[Transaction] TotalOpNum %d, SuccessOpNum %d FailedOpNum %d SuccessRate %f\n",
		taas_tikv.TotalTransactionCounter, taas_tikv.SuccessTransactionCounter, taas_tikv.FailedTransactionCounter,
		float64(taas_tikv.SuccessTransactionCounter)/float64(taas_tikv.TotalTransactionCounter))
	// fmt.Printf("[Op] ReadOpNum %d, UpdateOpNum %d UpdateRate %f\n",
	// 	workload.TotalReadCounter, workload.TotalUpdateCounter,
	// 	float64(workload.TotalUpdateCounter)/float64(workload.TotalReadCounter+workload.TotalUpdateCounter))
	// fmt.Printf("[Op] TikvTotalTime %d, TikvReadTime %d\n",
	// 	taas_tikv.TikvTotalLatency, taas_tikv.TikvReadLatency)
}

func runLoadCommandFunc(cmd *cobra.Command, args []string) {
	runClientCommandFunc(cmd, args, false, "load")
}

func runTransCommandFunc(cmd *cobra.Command, args []string) {
	runClientCommandFunc(cmd, args, true, "run")
}

var (
	threadsArg     int
	targetArg      int
	reportInterval int
)

func initClientCommand(m *cobra.Command) {
	m.Flags().StringSliceVarP(&propertyFiles, "property_file", "P", nil, "Spefify a property file")
	m.Flags().StringArrayVarP(&propertyValues, "prop", "p", nil, "Specify a property value with name=value")
	m.Flags().StringVar(&tableName, "table", "", "Use the table name instead of the default \""+prop.TableNameDefault+"\"")
	m.Flags().IntVar(&threadsArg, "threads", 1, "Execute using n threads - can also be specified as the \"threadcount\" property")
	m.Flags().IntVar(&targetArg, "target", 0, "Attempt to do n operations per second (default: unlimited) - can also be specified as the \"target\" property")
	m.Flags().IntVar(&reportInterval, "interval", 10, "Interval of outputting measurements in seconds")
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
