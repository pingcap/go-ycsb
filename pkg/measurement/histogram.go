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
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type histogram struct {
	boundCounts   util.ConcurrentMap
	boundInterval int64
	count         int64
	sum           int64
	min           int64
	max           int64
	startTime     time.Time
}

// Metric name.
const (
	HistogramBuckets        = "histogram.buckets"
	HistogramBucketsDefault = 1000
	ShardCount              = "cmap.shardCount"
	ShardCountDefault       = 32
	ELAPSED                 = "ELAPSED"
	COUNT                   = "COUNT"
	QPS                     = "QPS"
	AVG                     = "AVG"
	MIN                     = "MIN"
	MAX                     = "MAX"
	PER99TH                 = "PER99TH"
	PER999TH                = "PER999TH"
	PER9999TH               = "PER9999TH"
)

func (h *histogram) Info() ycsb.MeasurementInfo {
	res := h.getInfo()
	delete(res, ELAPSED)
	return newHistogramInfo(res)
}

func newHistogram(p *properties.Properties) *histogram {
	h := new(histogram)
	h.startTime = time.Now()
	h.boundCounts = util.New(p.GetInt(ShardCount, ShardCountDefault))
	h.boundInterval = p.GetInt64(HistogramBuckets, HistogramBucketsDefault)
	h.min = math.MaxInt64
	h.max = math.MinInt64
	return h
}

func (h *histogram) Measure(latency time.Duration) {
	n := int64(latency / time.Microsecond)

	atomic.AddInt64(&h.sum, n)
	atomic.AddInt64(&h.count, 1)
	bound := int(n / h.boundInterval)
	h.boundCounts.Upsert(bound, 1, func(ok bool, existedValue int64, newValue int64) int64 {
		if ok {
			return existedValue + newValue
		}
		return newValue
	})

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
	res := h.getInfo()

	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("Takes(s): %.1f, ", res[ELAPSED]))
	buf.WriteString(fmt.Sprintf("Count: %d, ", res[COUNT]))
	buf.WriteString(fmt.Sprintf("OPS: %.1f, ", res[QPS]))
	buf.WriteString(fmt.Sprintf("Avg(us): %d, ", res[AVG]))
	buf.WriteString(fmt.Sprintf("Min(us): %d, ", res[MIN]))
	buf.WriteString(fmt.Sprintf("Max(us): %d, ", res[MAX]))
	buf.WriteString(fmt.Sprintf("99th(us): %d, ", res[PER99TH]))
	buf.WriteString(fmt.Sprintf("99.9th(us): %d, ", res[PER999TH]))
	buf.WriteString(fmt.Sprintf("99.99th(us): %d", res[PER9999TH]))

	return buf.String()
}

func (h *histogram) getInfo() map[string]interface{} {
	min := atomic.LoadInt64(&h.min)
	max := atomic.LoadInt64(&h.max)
	sum := atomic.LoadInt64(&h.sum)
	count := atomic.LoadInt64(&h.count)

	bounds := h.boundCounts.Keys()
	sort.Ints(bounds)

	avg := int64(float64(sum) / float64(count))
	per99 := 0
	per999 := 0
	per9999 := 0

	opCount := int64(0)
	for _, bound := range bounds {
		boundCount, _ := h.boundCounts.Get(bound)
		opCount += boundCount
		per := float64(opCount) / float64(count)
		if per99 == 0 && per >= 0.99 {
			per99 = (bound + 1) * 1000
		}

		if per999 == 0 && per >= 0.999 {
			per999 = (bound + 1) * 1000
		}

		if per9999 == 0 && per >= 0.9999 {
			per9999 = (bound + 1) * 1000
		}
	}

	elapsed := time.Now().Sub(h.startTime).Seconds()
	qps := float64(count) / elapsed
	res := make(map[string]interface{})
	res[ELAPSED] = elapsed
	res[COUNT] = count
	res[QPS] = qps
	res[AVG] = avg
	res[MIN] = min
	res[MAX] = max
	res[PER99TH] = per99
	res[PER999TH] = per999
	res[PER9999TH] = per9999

	return res
}

type histogramInfo struct {
	info map[string]interface{}
}

func newHistogramInfo(info map[string]interface{}) *histogramInfo {
	return &histogramInfo{info: info}
}

func (hi *histogramInfo) Get(metricName string) interface{} {
	if value, ok := hi.info[metricName]; ok {
		return value
	}
	return nil
}
