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
	. "github.com/aerospike/aerospike-client-go/types"
)

type batchNode struct {
	Node    *Node
	offsets []int
	// offsetsSize     int
	BatchNamespaces []*batchNamespace
}

func newBatchNodeList(cluster *Cluster, policy *BatchPolicy, keys []*Key) ([]*batchNode, error) {
	nodes := cluster.GetNodes()

	if len(nodes) == 0 {
		return nil, NewAerospikeError(SERVER_NOT_AVAILABLE, "command failed because cluster is empty.")
	}

	// Create initial key capacity for each node as average + 25%.
	keysPerNode := len(keys) / len(nodes)
	keysPerNode += keysPerNode / 2

	// The minimum key capacity is 10.
	if keysPerNode < 10 {
		keysPerNode = 10
	}

	// Split keys by server node.
	batchNodes := make([]*batchNode, 0, len(nodes))

	for i := range keys {
		partition := NewPartitionByKey(keys[i])

		// error not required
		node, err := cluster.getMasterNode(partition)
		if err != nil {
			return nil, err
		}

		if batchNode := findBatchNode(batchNodes, node); batchNode == nil {
			batchNodes = append(batchNodes, newBatchNode(node, keysPerNode, i))
		} else {
			batchNode.AddKey(i)
		}
	}
	return batchNodes, nil
}

func newBatchIndexNodeList(cluster *Cluster, policy *BatchPolicy, records []*BatchRead) ([]*batchNode, error) {
	nodes := cluster.GetNodes()

	if len(nodes) == 0 {
		return nil, NewAerospikeError(SERVER_NOT_AVAILABLE, "command failed because cluster is empty.")
	}

	// Create initial key capacity for each node as average + 25%.
	keysPerNode := len(records) / len(nodes)
	keysPerNode += keysPerNode / 2

	// The minimum key capacity is 10.
	if keysPerNode < 10 {
		keysPerNode = 10
	}

	// Split keys by server node.
	batchNodes := make([]*batchNode, 0, len(nodes))

	for i := range records {
		partition := NewPartitionByKey(records[i].Key)

		// error not required
		node, err := cluster.getMasterNode(partition)
		if err != nil {
			return nil, err
		}

		if batchNode := findBatchNode(batchNodes, node); batchNode == nil {
			batchNodes = append(batchNodes, newBatchNode(node, keysPerNode, i))
		} else {
			batchNode.AddKey(i)
		}
	}
	return batchNodes, nil
}

func newBatchNode(node *Node, capacity int, offset int) *batchNode {
	res := &batchNode{
		Node:    node,
		offsets: make([]int, 1, capacity),
		// offsetsSize:     1,
		BatchNamespaces: nil, //[]*batchNamespace{newBatchNamespace(namespace, keyCapacity, offset)},
	}

	res.offsets[0] = offset
	return res
}

func (bn *batchNode) AddKey(offset int) {
	bn.offsets = append(bn.offsets, offset)
	// bn.offsetsSize++
}

func (bn *batchNode) findNamespace(batchNamespaces []*batchNamespace, ns string) *batchNamespace {
	for _, batchNamespace := range batchNamespaces {
		// Note: use both pointer equality and equals.
		if batchNamespace.namespace == ns {
			return batchNamespace
		}
	}
	return nil
}

func findBatchNode(nodes []*batchNode, node *Node) *batchNode {
	for i := range nodes {
		// Note: using pointer equality for performance.
		if nodes[i].Node == node {
			return nodes[i]
		}
	}
	return nil
}

func (bn *batchNode) splitByNamespace(keys []*Key) {
	first := keys[bn.offsets[0]].namespace

	// Optimize for single namespace.
	if bn.isSingleNamespace(keys, first) {
		bn.BatchNamespaces = []*batchNamespace{newBatchNamespace(first, bn.offsets)}
		return
	}

	// Process multiple namespaces.
	bn.BatchNamespaces = make([]*batchNamespace, 0, 4)

	for i := range bn.offsets {
		offset := bn.offsets[i]
		ns := keys[offset].namespace
		batchNamespace := bn.findNamespace(bn.BatchNamespaces, ns)

		if batchNamespace == nil {
			bn.BatchNamespaces = append(bn.BatchNamespaces, newBatchNamespaceDirect(ns, len(bn.offsets), offset))
		} else {
			batchNamespace.add(offset)
		}
	}
}

func (bn *batchNode) isSingleNamespace(keys []*Key, first string) bool {
	for i := range bn.offsets {
		ns := keys[bn.offsets[i]].namespace

		if ns != first {
			return false
		}
	}
	return true
}

type batchNamespace struct {
	namespace string
	offsets   []int
}

func newBatchNamespaceDirect(namespace string, capacity, offset int) *batchNamespace {
	res := &batchNamespace{
		namespace: namespace,
		offsets:   make([]int, 1, capacity),
	}
	res.offsets[0] = offset
	return res
}

func newBatchNamespace(namespace string, offsets []int) *batchNamespace {
	return &batchNamespace{
		namespace: namespace,
		offsets:   offsets,
	}
}

func (bn *batchNamespace) add(offset int) {
	bn.offsets = append(bn.offsets, offset)
}
