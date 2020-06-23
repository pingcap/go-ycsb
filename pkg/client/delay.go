// Copyright 2020 PingCAP, Inc.
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

package client

import (
	"math"
	"math/rand"
	"time"
)

var oldtime int = -1
var noiseRange float64 = 0

// If radio is y，noiseRange is rand float64 in [-y/(1+y),y/(1-y)]
func getnoiseRange(newtime int, ratio float64) {
	// Not locked, not necessary
	if newtime != oldtime {
		if ratio >= 1 {
			// [-1/2,4]
			noiseRange = float64(rand.Int63n(5)-1) / 2
		} else if ratio <= 0 {
			noiseRange = 0
		} else {
			// [-y/(1+y),y/(1-y)]
			noiseRange = float64(rand.Int63n(int64(10000*2*ratio))-int64(10000*ratio*(1-ratio))) / (10000 * (1 - ratio*ratio))
			if noiseRange > 4 {
				noiseRange = 4
			}
		}
		oldtime = newtime
	}
}

func (w *worker) Normal(loadStartTime time.Time) {
	w.delay = int64(float64(w.threadCount) * float64(time.Second) / (float64(w.expectedOps) * math.Sqrt(2*math.Pi) * w.std))
	timeNow := int(time.Now().Sub(loadStartTime).Minutes()) % w.period
	v := 1 / (math.Sqrt(2*math.Pi) * w.std) * math.Pow(math.E, (-math.Pow((float64(timeNow)-w.mean), 2)/(2*math.Pow(w.std, 2))))
	tmp := 1 / v * float64(w.delay)
	if tmp > 3*math.Pow(10.0, 10) {
		tmp = 3 * math.Pow(10.0, 10)
	}
	time.Sleep(time.Duration(tmp))
}

func (w *worker) Noise_normal(loadStartTime time.Time) {
	w.delay = int64(float64(w.threadCount) * float64(time.Second) / (float64(w.expectedOps) * math.Sqrt(2*math.Pi) * w.std))
	timeNow := int(time.Now().Sub(loadStartTime).Minutes()) % w.period
	v := 1 / (math.Sqrt(2*math.Pi) * w.std) * math.Pow(math.E, (-math.Pow((float64(timeNow)-w.mean), 2)/(2*math.Pow(w.std, 2))))
	tmp := 1 / v * float64(w.delay)
	if tmp > 3*math.Pow(10.0, 10) {
		tmp = 3 * math.Pow(10.0, 10)
	}
	getnoiseRange(timeNow, w.noiseRatio)
	noise := tmp * noiseRange
	time.Sleep(time.Duration(tmp + noise))
}

func (w *worker) Step(loadStartTime time.Time) {
	w.delay = int64(float64(w.threadCount) * float64(time.Second) / float64(w.expectedOps))
	timeNow := int(time.Now().Sub(loadStartTime).Minutes()) % w.period
	v := 3 - int(float64(timeNow*3.0/w.period))
	tmp := int64(v) * w.delay
	time.Sleep(time.Duration(tmp))
}

func (w *worker) Noise_step(loadStartTime time.Time) {
	w.delay = int64(float64(w.threadCount) * float64(time.Second) / float64(w.expectedOps))
	timeNow := int(time.Now().Sub(loadStartTime).Minutes()) % w.period
	v := 3 - int(float64(timeNow*3.0/w.period))
	tmp := float64(v) * float64(w.delay)
	getnoiseRange(timeNow, w.noiseRatio)
	noise := tmp * noiseRange
	time.Sleep(time.Duration(tmp + noise))
}

func (w *worker) Meituan(loadStartTime time.Time) {
	timeNow := int(time.Now().Sub(loadStartTime).Minutes()) % w.period
	x := 24.0 * float64(timeNow) / float64(w.period) //Simulate 24 hours a day
	var y float64 = 0
	switch {
	case x <= 2:
		y = 0
	case x <= 6:
		y = (x - 2.0) / 4
	case x <= 8:
		y = 2.8 - 0.3*x
	case x <= 16:
		y = 0.4
	case x <= 18:
		y = 0.2*x - 2.8
	case x <= 24:
		y = 3.2 - 0.8*x/6.0
	default:
		y = 1
	}
	Q := y * float64(w.expectedOps)
	if Q < float64(w.threadCount) {
		Q = float64(w.threadCount)
	}
	w.delay = int64(float64(w.threadCount) * float64(time.Second) / Q)
	getnoiseRange(timeNow, w.noiseRatio)
	noise := int64(float64(w.delay) * noiseRange)
	time.Sleep(time.Duration(w.delay + noise))
}
