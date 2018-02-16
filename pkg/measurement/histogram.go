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

package measurement

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"sync/atomic"
	"time"

	"github.com/magiconair/properties"
)

type histogram struct {
	upperBounds []int
	counts      []int64
	count       int64
	sum         int64
	min         int64
	max         int64
}

func newHistogram(_ *properties.Properties) *histogram {
	h := new(histogram)
	// TODO: support defining buckets from properties
	// bucket unit is 1ms
	h.upperBounds = make([]int, 1024)
	for i := 0; i < len(h.upperBounds); i++ {
		h.upperBounds[i] = (i + 1) * 1000
	}
	h.counts = make([]int64, len(h.upperBounds))
	h.min = math.MaxInt64
	return h
}

func (h *histogram) Measure(lantecy time.Duration) {
	n := int64(lantecy / time.Microsecond)

	atomic.AddInt64(&h.sum, n)
	atomic.AddInt64(&h.count, 1)

	i := sort.SearchInts(h.upperBounds, int(n))
	if i < len(h.counts) {
		atomic.AddInt64(&h.counts[i], 1)
	}

	for {
		oldMin := atomic.LoadInt64(&h.min)
		if n >= oldMin {
			break
		}

		if atomic.CompareAndSwapInt64(&h.min, oldMin, n) {
			break
		}
	}

	for {
		oldMax := atomic.LoadInt64(&h.max)
		if n <= oldMax {
			break
		}

		if atomic.CompareAndSwapInt64(&h.max, oldMax, n) {
			break
		}
	}
}

func (h *histogram) Summary() string {
	min := atomic.LoadInt64(&h.min)
	max := atomic.LoadInt64(&h.max)
	sum := atomic.LoadInt64(&h.sum)
	count := atomic.LoadInt64(&h.count)

	counts := make([]int64, len(h.counts))
	for i := 0; i < len(h.upperBounds); i++ {
		counts[i] = atomic.LoadInt64(&h.counts[i])
	}

	avg := int64(float64(sum) / float64(count))
	per95 := int(0)
	per99 := int(0)

	opCount := int64(0)
	for i := 0; i < len(counts); i++ {
		opCount += counts[i]
		per := float64(opCount) / float64(count)
		if per95 == 0 && per >= 0.95 {
			per95 = h.upperBounds[i]
		}

		if per99 == 0 && per >= 0.99 {
			per99 = h.upperBounds[i]
		}
	}

	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("Count: %d, ", count))
	buf.WriteString(fmt.Sprintf("Avg(us): %d, ", avg))
	buf.WriteString(fmt.Sprintf("Min(us): %d, ", min))
	buf.WriteString(fmt.Sprintf("Max(us): %d, ", max))
	buf.WriteString(fmt.Sprintf("95th(us): %d, ", per95))
	buf.WriteString(fmt.Sprintf("99th(us): %d", per99))

	return buf.String()
}
