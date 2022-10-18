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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

var header = []string{"Operation", "Takes(s)", "Count", "OPS", "Avg(us)", "Min(us)", "Max(us)", "99th(us)", "99.9th(us)", "99.99th(us)"}

type measurement struct {
	sync.RWMutex

	p *properties.Properties

	opMeasurement ycsb.Measurement
}

func (m *measurement) measure(op string, start time.Time, lan time.Duration) {
	m.Lock()
	m.opMeasurement.Measure(op, start, lan)
	m.Unlock()
}

func (m *measurement) output() {
	m.RLock()
	defer m.RUnlock()

	summaries := m.opMeasurement.Summary()
	keys := make([]string, 0, len(summaries))
	var i = 0
	for k := range summaries {
		keys[i] = k
		i += 1
	}
	sort.Strings(keys)

	lines := [][]string{}
	for _, op := range keys {
		line := []string{op}
		line = append(line, summaries[op]...)
		lines = append(lines, line)
	}

	outputStyle := m.p.GetString(prop.OutputStyle, util.OutputStylePlain)
	switch outputStyle {
	case util.OutputStylePlain:
		util.RenderString("%-6s - %s\n", header, lines)
	case util.OutputStyleJson:
		util.RenderJson(header, lines)
	case util.OutputStyleTable:
		util.RenderTable(header, lines)
	default:
		panic("unsupported outputstyle: " + outputStyle)
	}
}

func (m *measurement) info() map[string]ycsb.MeasurementInfo {
	m.RLock()
	defer m.RUnlock()

	return m.opMeasurement.Info()
}

func (m *measurement) getOpName() []string {
	m.RLock()
	defer m.RUnlock()

	return m.opMeasurement.OpNames()
}

// InitMeasure initializes the global measurement.
func InitMeasure(p *properties.Properties) {
	globalMeasure = new(measurement)
	globalMeasure.p = p
	measurementType := p.GetString(prop.MeasurementType, "")
	switch measurementType {
	case "histogram":
		globalMeasure.opMeasurement = InitHistograms(p)
	default:
		panic("unsupported measurement type: " + measurementType)
	}
	EnableWarmUp(p.GetInt64(prop.WarmUpTime, 0) > 0)
}

// Output prints the measurement summary.
func Output() {
	globalMeasure.output()
}

// EnableWarmUp sets whether to enable warm-up.
func EnableWarmUp(b bool) {
	if b {
		atomic.StoreInt32(&warmUp, 1)
	} else {
		atomic.StoreInt32(&warmUp, 0)
	}
}

// IsWarmUpFinished returns whether warm-up is finished or not.
func IsWarmUpFinished() bool {
	return atomic.LoadInt32(&warmUp) == 0
}

// Measure measures the operation.
func Measure(op string, start time.Time, lan time.Duration) {
	if IsWarmUpFinished() {
		globalMeasure.measure(op, start, lan)
	}
}

// Info returns all the operations MeasurementInfo.
// The key of returned map is the operation name.
func Info() map[string]ycsb.MeasurementInfo {
	return globalMeasure.info()
}

// GetOpNames returns a string slice which contains all the operation name measured.
func GetOpNames() []string {
	return globalMeasure.getOpName()
}

var globalMeasure *measurement
var warmUp int32 // use as bool, 1 means in warmup progress, 0 means warmup finished.
