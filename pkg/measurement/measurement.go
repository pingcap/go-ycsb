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
	"fmt"
	"sync"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type measurement struct {
	sync.RWMutex

	p *properties.Properties

	opMeasurement map[string]ycsb.Measurement
}

func (m *measurement) Measure(op string, lan time.Duration) {
	m.RLock()
	opM, ok := m.opMeasurement[op]
	m.RUnlock()

	if !ok {
		opM = newHistogram(m.p)
		m.Lock()
		m.opMeasurement[op] = opM
		m.Unlock()
	}

	opM.Measure(lan)
}

func (m *measurement) Output() {
	m.RLock()
	defer m.RUnlock()

	for op, opM := range m.opMeasurement {
		fmt.Printf("%s - %s\n", op, opM.Summary())
	}
}

// InitMeasure initializes the global measurement.
func InitMeasure(p *properties.Properties) {
	globalMeasure = new(measurement)
	globalMeasure.p = p
	globalMeasure.opMeasurement = make(map[string]ycsb.Measurement, 16)
}

// Output prints the measurement summary.
func Output() {
	globalMeasure.Output()
}

// Measure measures the operation.
func Measure(op string, lan time.Duration) {
	globalMeasure.Measure(op, lan)
}

var globalMeasure *measurement
