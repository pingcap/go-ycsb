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

/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package generator

import (
	"fmt"
	"math"
	"math/rand"
	"sync"

	"github.com/pingcap/go-ycsb/pkg/util"
)

//  const (
// 	 // ParetoTheta is the 'theta' of Generized Pareto Distribution.
// 	 ParetoTheta = float64(0)
// 	 // ParetoK is the 'k' of Generized Pareto Distribution.
// 	 ParetoK = float64(0.923)
// 	 // ParetoSigma is the 'sigma' of Generized Pareto Distribution.
// 	 ParetoSigma = float64(226.409)
//  )

type keyRangeUnit struct {
	keyRangeStart  int64
	keyRangeAccess int64
	keyRangeKeys   int64
}

// TwoTermExpKeys generates integers follow TwoTermExpKeys distribution.
type TwoTermExpKeys struct {
	Number
	keyRangeRandMax int64
	keyRangeSize    int64
	keyRangeNum     int64
	keyRangeSet     []*keyRangeUnit
	zipfGen         *ScrambledZipfian
	bottomIndex     int64
	randLocal       *rand.Rand
	sync.RWMutex
}

// NewTwoTermExpKeys creates the NewTwoTermExpKeys generator.
func NewTwoTermExpKeys(zipfianRange bool, totalKeys int64, keyRangeNum int64, prefixA float64, prefixB float64, prefixC float64, prefixD float64) *TwoTermExpKeys {
	ttek := &TwoTermExpKeys{}

	var amplify int64
	var keyRangeStart int64
	if keyRangeNum <= 0 {
		ttek.keyRangeNum = 1
	} else {
		ttek.keyRangeNum = keyRangeNum
	}
	ttek.keyRangeSize = totalKeys / ttek.keyRangeNum

	for pfx := ttek.keyRangeNum; pfx >= 1; pfx-- {
		keyRangeP := prefixA*math.Exp(prefixB*float64(pfx)) + prefixC*math.Exp(prefixD*float64(pfx))
		if zipfianRange {
			keyRangeP = 1.0 / math.Pow(float64(pfx), 0.99)
		}

		if keyRangeP < math.Pow10(-16) {
			keyRangeP = float64(0)
		}
		if amplify == 0 && keyRangeP > 0 {
			amplify = int64(math.Floor(1/keyRangeP)) + 1
		}

		pUnit := &keyRangeUnit{}
		pUnit.keyRangeStart = keyRangeStart
		if 0.0 >= keyRangeP {
			pUnit.keyRangeAccess = 0
		} else {
			pUnit.keyRangeAccess = int64(math.Floor(float64(amplify) * keyRangeP))
		}
		pUnit.keyRangeKeys = ttek.keyRangeSize
		ttek.keyRangeSet = append(ttek.keyRangeSet, pUnit)
		keyRangeStart += pUnit.keyRangeAccess

		// fmt.Println("key range ", pfx, " access weight : ", pUnit.keyRangeAccess)
	}
	ttek.keyRangeRandMax = keyRangeStart

	bottomIndex := int64(0)
	for ; bottomIndex < ttek.keyRangeNum; bottomIndex++ {
		pr := float64(ttek.keyRangeSet[bottomIndex].keyRangeAccess) / float64(ttek.keyRangeRandMax)
		if pr >= 0.01 {
			break
		}
	}

	ttek.randLocal = rand.New(rand.NewSource(ttek.keyRangeRandMax))
	for i := int64(0); i < keyRangeNum; i++ {
		pos := ttek.randLocal.Int63n(keyRangeNum)

		if i >= bottomIndex || pos >= bottomIndex {
			continue
		}

		tmp := ttek.keyRangeSet[i]
		ttek.keyRangeSet[i] = ttek.keyRangeSet[pos]
		ttek.keyRangeSet[pos] = tmp
	}

	ttek.bottomIndex = bottomIndex

	offset := int64(0)
	for _, pUnit := range ttek.keyRangeSet {
		pUnit.keyRangeStart = offset
		offset += pUnit.keyRangeAccess
	}

	return ttek
}

// Next implements the Generator Next interface.
func (t *TwoTermExpKeys) Next(r *rand.Rand) int64 {
	return 0
}

// Adjust adjusts key range
func (t *TwoTermExpKeys) Adjust(smallValPos, bigValPos []int64) {
	halfNum := (t.keyRangeNum - t.bottomIndex) / 2
	j := 0
	for i := t.bottomIndex; i < t.bottomIndex+halfNum; i++ {
		pos := bigValPos[j]
		j++
		if j >= len(bigValPos) {
			break
		}
		tmp := t.keyRangeSet[i]
		t.keyRangeSet[i] = t.keyRangeSet[pos]
		t.keyRangeSet[pos] = tmp
	}

	j = 0
	for i := t.bottomIndex + halfNum; i < t.keyRangeNum; i++ {
		pos := smallValPos[j]
		j++
		if j >= len(smallValPos) {
			break
		}
		tmp := t.keyRangeSet[i]
		t.keyRangeSet[i] = t.keyRangeSet[pos]
		t.keyRangeSet[pos] = tmp
	}

	offset := int64(0)
	for _, pUnit := range t.keyRangeSet {
		pUnit.keyRangeStart = offset
		offset += pUnit.keyRangeAccess
	}
}

func (t *TwoTermExpKeys) Shuffle() {
	t.Lock()
	defer t.Unlock()

	for i := int64(0); i < t.keyRangeNum; i++ {
		pos := t.randLocal.Int63n(t.keyRangeNum)

		tmp := t.keyRangeSet[i]
		t.keyRangeSet[i] = t.keyRangeSet[pos]
		t.keyRangeSet[pos] = tmp
	}

	offset := int64(0)
	for _, pUnit := range t.keyRangeSet {
		pUnit.keyRangeStart = offset
		offset += pUnit.keyRangeAccess
	}

}

// PrintKeyRangeInfo prints key range info.
func (t *TwoTermExpKeys) PrintKeyRangeInfo(valSizes []int64) {
	fmt.Println("KeyRangeDist: total access weight", t.keyRangeRandMax)
	for i := int64(0); i < t.keyRangeNum; i++ {
		pr := float64(t.keyRangeSet[i].keyRangeAccess) / float64(t.keyRangeRandMax)
		if pr > 0.005 {
			fmt.Printf("KeyRangeDist: range %v, start_key %v, access probability %v, value size %v\n", i, t.keyRangeSize*i, pr, valSizes[i])
		}
	}
}

// DistGetKeyID implements DistGetKeyID.
func (t *TwoTermExpKeys) DistGetKeyID(useZipf bool, r *rand.Rand, initRand int64, keyDistA float64, keyDistB float64) int64 {
	keyRangeRand := util.Hash64(initRand) % t.keyRangeRandMax

	start := 0
	end := len(t.keyRangeSet)
	for start+1 < end {
		mid := start + (end-start)/2
		if keyRangeRand < t.keyRangeSet[mid].keyRangeStart {
			end = mid
		} else {
			start = mid
		}
	}
	keyRangeID := start

	if useZipf {
		keyRangeID = int(t.zipfGen.Next(r))
	}

	var keyOffset, keySeed int64
	if keyDistA == 0.0 || keyDistB == 0.0 {
		keyOffset = initRand % t.keyRangeSize
	} else {
		u := float64(initRand%t.keyRangeSize) / float64(t.keyRangeSize)
		keySeed = int64(math.Ceil(math.Pow(u/keyDistA, 1/keyDistB)))
		// randKey := rand.New(rand.NewSource(keySeed))
		// keyOffset = randKey.Int63n(t.keyRangeSize)
		keyOffset = util.Hash64(keySeed) % t.keyRangeSize
	}
	return t.keyRangeSize*int64(keyRangeID) + keyOffset
}
