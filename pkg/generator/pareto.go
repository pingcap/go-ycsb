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
	"math"
	"math/rand"
)

const (
	// ParetoTheta is the 'theta' of Generized Pareto Distribution.
	ParetoTheta = float64(0)
	// ParetoK is the 'k' of Generized Pareto Distribution.
	ParetoK = float64(0.923)
	// ParetoSigma is the 'sigma' of Generized Pareto Distribution.
	ParetoSigma = float64(226.409)
)

// Pareto generates integers follow Pareto distribution.
// "f(x)=(1/sigma)*(1+k*(x-theta)/sigma)^-(1/k+1)");
type Pareto struct {
	Number
	maxValue int64
	theta    float64
	k        float64
	sigma    float64
}

// NewPareto creates the Pareto generator.
func NewPareto(maxValue int64, theta float64, k float64, sigma float64) *Pareto {
	return &Pareto{
		maxValue: maxValue,
		theta:    theta,
		k:        k,
		sigma:    sigma,
	}
}

// Next implements the Generator Next interface.
func (p *Pareto) Next(r *rand.Rand) int64 {
	u := r.Float64()
	val := 0.0
	if p.k == 0.0 {
		val = p.theta - p.sigma*math.Log(u)
	} else {
		val = p.theta + p.sigma*(math.Pow(u, -1*p.k)-1)/p.k
	}
	ret := int64(math.Ceil(val))
	if ret <= 0 {
		ret = 10
	} else if ret > p.maxValue {
		ret = ret % p.maxValue
	}
	return ret
}
