/*
 * Copyright 2013-2017 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package aerospike

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"sync"

	. "github.com/aerospike/aerospike-client-go/logger"
	. "github.com/aerospike/aerospike-client-go/types"
)

const (
	_PartitionGeneration = "partition-generation"
	_ReplicasMaster      = "replicas-master"
	_Replicas            = "replicas"
	_ReplicasAll         = "replicas-all"
)

var partitionMapLock sync.Mutex

// Parse node's master (and optionally prole) partitions.
type partitionParser struct {
	pmap           partitionMap
	buffer         []byte
	partitionCount int
	generation     int
	length         int
	offset         int
}

func newPartitionParser(node *Node, partitions partitionMap, partitionCount int, requestProleReplicas bool) (*partitionParser, error) {
	newPartitionParser := &partitionParser{
		partitionCount: partitionCount,
	}

	// Send format 1:  partition-generation\nreplicas\n
	// Send format 2:  partition-generation\nreplicas-all\n
	// Send format 3:  partition-generation\nreplicas-master\n
	command := _ReplicasMaster
	if node.supportsReplicas.Get() {
		command = _Replicas
	} else if requestProleReplicas {
		command = _ReplicasAll
	}

	info, err := node.requestRawInfo(_PartitionGeneration, command)
	if err != nil {
		return nil, err
	}

	newPartitionParser.buffer = info.msg.Data
	newPartitionParser.length = len(info.msg.Data)
	if newPartitionParser.length == 0 {
		return nil, NewAerospikeError(PARSE_ERROR, fmt.Sprintf("Partition info is empty"))
	}

	newPartitionParser.generation, err = newPartitionParser.parseGeneration()
	if err != nil {
		return nil, err
	}

	newPartitionParser.pmap = partitions

	partitionMapLock.Lock()
	defer partitionMapLock.Unlock()

	if node.supportsReplicas.Get() {
		err = newPartitionParser.parseReplicasAll(node, command)
	} else if requestProleReplicas {
		err = newPartitionParser.parseReplicasAll(node, command)
	} else {
		err = newPartitionParser.parseReplicasMaster(node)
	}

	if err != nil {
		return nil, err
	}

	return newPartitionParser, nil
}

func (pp *partitionParser) getGeneration() int {
	return pp.generation
}

func (pp *partitionParser) getPartitionMap() partitionMap {
	return pp.pmap
}

func (pp *partitionParser) parseGeneration() (int, error) {
	if err := pp.expectName(_PartitionGeneration); err != nil {
		return -1, err
	}

	begin := pp.offset
	for pp.offset < pp.length {
		if pp.buffer[pp.offset] == '\n' {
			s := string(pp.buffer[begin:pp.offset])
			pp.offset++
			return strconv.Atoi(s)
		}
		pp.offset++
	}
	return -1, NewAerospikeError(PARSE_ERROR, fmt.Sprintf("Failed to find partition-generation value"))
}

func (pp *partitionParser) parseReplicasMaster(node *Node) error {
	// Use low-level info methods and parse byte array directly for maximum performance.
	// Receive format: replicas-master\t<ns1>:<base 64 encoded bitmap1>;<ns2>:<base 64 encoded bitmap2>...\n
	if err := pp.expectName(_ReplicasMaster); err != nil {
		return err
	}

	begin := pp.offset

	for pp.offset < pp.length {
		if pp.buffer[pp.offset] == ':' {
			// Parse namespace.
			namespace := string(pp.buffer[begin:pp.offset])

			if len(namespace) <= 0 || len(namespace) >= 32 {
				response := pp.getTruncatedResponse()
				return NewAerospikeError(PARSE_ERROR, fmt.Sprintf("Invalid partition namespace `%s` response: `%s`", namespace, response))
			}
			pp.offset++
			begin = pp.offset

			// Parse partition bitmap.
			for pp.offset < pp.length {
				b := pp.buffer[pp.offset]

				if b == ';' || b == '\n' {
					break
				}
				pp.offset++
			}

			if pp.offset == begin {
				response := pp.getTruncatedResponse()
				return NewAerospikeError(PARSE_ERROR, fmt.Sprintf("Empty partition id for namespace `%s` response: `%s`", namespace, response))
			}

			partitions := pp.pmap[namespace]
			if partitions == nil {
				// Create new replica array.
				partitions = newPartitions(pp.partitionCount, 1, false)
				pp.pmap[namespace] = partitions
			}

			if err := pp.decodeBitmap(node, partitions, 0, 0, begin); err != nil {
				return err
			}
			pp.offset++
			begin = pp.offset
		} else {
			pp.offset++
		}
	}

	return nil
}

func (pp *partitionParser) parseReplicasAll(node *Node, command string) error {
	// Use low-level info methods and parse byte array directly for maximum performance.
	// Receive format: replicas-all\t
	//                 <ns1>:[regime],<count>,<base 64 encoded bitmap1>,<base 64 encoded bitmap2>...;
	//                 <ns2>:[regime],<count>,<base 64 encoded bitmap1>,<base 64 encoded bitmap2>...;\n
	if err := pp.expectName(command); err != nil {
		return err
	}

	begin := pp.offset
	regime := 0

	for pp.offset < pp.length {
		if pp.buffer[pp.offset] == ':' {
			// Parse namespace.
			namespace := string(pp.buffer[begin:pp.offset])

			if len(namespace) <= 0 || len(namespace) >= 32 {
				response := pp.getTruncatedResponse()
				return NewAerospikeError(PARSE_ERROR, fmt.Sprintf("Invalid partition namespace `%s` response: `%s`", namespace, response))
			}
			pp.offset++
			begin = pp.offset

			// Parse regime.
			if command == _Replicas {
				for pp.offset < pp.length {
					b := pp.buffer[pp.offset]

					if b == ',' {
						break
					}
					pp.offset++
				}

				var err error
				regime, err = strconv.Atoi(string(pp.buffer[begin:pp.offset]))
				if err != nil {
					return err
				}

				pp.offset++
				begin = pp.offset
			}

			// Parse replica count.
			for pp.offset < pp.length {
				b := pp.buffer[pp.offset]

				if b == ',' {
					break
				}
				pp.offset++
			}

			replicaCount, err := strconv.Atoi(string(pp.buffer[begin:pp.offset]))
			if err != nil {
				return err
			}

			partitions := pp.pmap[namespace]
			if partitions == nil {
				// Create new replica array.
				partitions = newPartitions(pp.partitionCount, replicaCount, regime != 0)
				pp.pmap[namespace] = partitions
			} else if len(partitions.Replicas) != replicaCount {
				// Ensure replicaArray is correct size.
				Logger.Info("Namespace `%s` replication factor changed from `%d` to `%d` ", namespace, len(partitions.Replicas), replicaCount)

				partitions.setReplicaCount(replicaCount) //= clonePartitions(partitions, replicaCount)
				pp.pmap[namespace] = partitions
			}

			// Parse partition bitmaps.
			for i := 0; i < replicaCount; i++ {
				pp.offset++
				begin = pp.offset

				// Find bitmap endpoint
				for pp.offset < pp.length {
					b := pp.buffer[pp.offset]

					if b == ',' || b == ';' {
						break
					}
					pp.offset++
				}

				if pp.offset == begin {
					response := pp.getTruncatedResponse()
					return NewAerospikeError(PARSE_ERROR, fmt.Sprintf("Empty partition id for namespace `%s` response: `%s`", namespace, response))
				}

				if err := pp.decodeBitmap(node, partitions, i, regime, begin); err != nil {
					return err
				}
			}
			pp.offset++
			begin = pp.offset
		} else {
			pp.offset++
		}
	}

	return nil
}

func (pp *partitionParser) decodeBitmap(node *Node, partitions *Partitions, replica int, regime int, begin int) error {
	restoreBuffer, err := base64.StdEncoding.DecodeString(string(pp.buffer[begin:pp.offset]))
	if err != nil {
		return err
	}

	for partition := 0; partition < pp.partitionCount; partition++ {
		nodeOld := partitions.Replicas[replica][partition]

		if (restoreBuffer[partition>>3] & (0x80 >> uint(partition&7))) != 0 {
			// Node owns this partition.
			regimeOld := partitions.regimes[partition]

			if regime == 0 || regime >= regimeOld {
				if regime > regimeOld {
					partitions.regimes[partition] = regime
				}

				if nodeOld != nil && nodeOld != node {
					// Force previously mapped node to refresh it's partition map on next cluster tend.
					nodeOld.partitionGeneration.Set(-1)
				}

				partitions.Replicas[replica][partition] = node
			}
		} else {
			// Node does not own partition.
			if node == nodeOld {
				// Must erase previous map.
				partitions.Replicas[replica][partition] = nil
			}
		}
	}

	return nil
}

func (pp *partitionParser) expectName(name string) error {
	begin := pp.offset

	for pp.offset < pp.length {
		if pp.buffer[pp.offset] == '\t' {
			s := string(pp.buffer[begin:pp.offset])
			if name == s {
				pp.offset++
				return nil
			}
			break
		}
		pp.offset++
	}

	return NewAerospikeError(PARSE_ERROR, fmt.Sprintf("Failed to find `%s`", name))
}

func (pp *partitionParser) getTruncatedResponse() string {
	max := pp.length
	if max > 200 {
		max = 200
	}
	return string(pp.buffer[0:max])
}
