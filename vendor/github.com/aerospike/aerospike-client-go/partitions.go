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
	"bytes"
	"strconv"
)

type Partitions struct {
	Replicas [][]*Node
	CPMode   bool
	regimes  []int
}

func newPartitions(partitionCount int, replicaCount int, cpMode bool) *Partitions {
	replicas := make([][]*Node, replicaCount)
	for i := range replicas {
		replicas[i] = make([]*Node, partitionCount)
	}

	return &Partitions{
		Replicas: replicas,
		CPMode:   cpMode,
		regimes:  make([]int, partitionCount),
	}
}

// Copy partition map while reserving space for a new replica count.
func clonePartitions(other *Partitions, replicaCount int) *Partitions {
	replicas := make([][]*Node, replicaCount)

	if len(other.Replicas) < replicaCount {
		// Copy existing entries.
		i := copy(replicas, other.Replicas)

		// Create new entries.
		for ; i < replicaCount; i++ {
			replicas[i] = make([]*Node, len(other.regimes))
		}
	} else {
		// Copy existing entries.
		copy(replicas, other.Replicas)
	}

	regimes := make([]int, len(other.regimes))
	copy(regimes, other.regimes)

	return &Partitions{
		Replicas: replicas,
		CPMode:   other.CPMode,
		regimes:  regimes,
	}
}

/*

	partitionMap

*/

type partitionMap map[string]*Partitions

// String implements stringer interface for partitionMap
func (pm partitionMap) clone() partitionMap {
	// Make shallow copy of map.
	pmap := make(partitionMap, len(pm))
	for ns, _ := range pm {
		pmap[ns] = clonePartitions(pm[ns], len(pm[ns].Replicas))
	}
	return pmap
}

// String implements stringer interface for partitionMap
func (pm partitionMap) merge(other partitionMap) {
	// merge partitions; iterate over the new partition and update the old one
	for ns, partitions := range other {
		replicaArray := partitions.Replicas
		if pm[ns] == nil {
			pm[ns] = clonePartitions(partitions, len(replicaArray))
		} else {
			for i, nodeArray := range replicaArray {
				if len(pm[ns].Replicas) <= i {
					pm[ns].Replicas = append(pm[ns].Replicas, make([]*Node, len(nodeArray)))
				} else if pm[ns].Replicas[i] == nil {
					pm[ns].Replicas[i] = make([]*Node, len(nodeArray))
				}

				for j, node := range nodeArray {
					if node != nil {
						pm[ns].Replicas[i][j] = node
					}
				}
			}
		}

		if len(pm[ns].regimes) < len(partitions.regimes) {
			// expand regime size array
			regimes := make([]int, len(partitions.regimes))
			copy(regimes, pm[ns].regimes)
			pm[ns].regimes = regimes
		}

		// merge regimes
		for i := range partitions.regimes {
			if pm[ns].regimes[i] < partitions.regimes[i] {
				pm[ns].regimes[i] = partitions.regimes[i]
			}
		}

	}
}

// String implements stringer interface for partitionMap
func (pm partitionMap) String() string {
	res := bytes.Buffer{}
	for ns, partitions := range pm {
		replicaArray := partitions.Replicas
		for i, nodeArray := range replicaArray {
			for j, node := range nodeArray {
				res.WriteString(ns)
				res.WriteString(",")
				res.WriteString(strconv.Itoa(i))
				res.WriteString(",")
				res.WriteString(strconv.Itoa(j))
				res.WriteString(",")
				if node != nil {
					res.WriteString(node.String())
				} else {
					res.WriteString("NIL")
				}
				res.WriteString("\n")
			}
		}
	}
	return res.String()
}
