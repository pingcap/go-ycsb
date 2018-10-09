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

package ycsb

import (
	"time"
)

// MeasurementInfo contains metrics of one measurement.
type MeasurementInfo interface {
	// Get returns the value corresponded to the specified metric, such QPS, MIN, MAX，etc.
	// If metric does not exist, the returned value will be nil.
	Get(metricName string) interface{}
}

// Measurement measures the operations metrics.
type Measurement interface {
	// Measure measures the operation latency.
	Measure(latency time.Duration)
	// Summary returns the summary of the measurement.
	Summary() string
	// Info returns the MeasurementInfo of the measurement.
	Info() MeasurementInfo
}
