/*
 * ContinuousSample.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef CONTINUOUSSAMPLE_H
#define CONTINUOUSSAMPLE_H
#pragma once

#include "flow/Platform.h"
#include "flow/IRandom.h"
#include <vector>
#include <algorithm>

template <class T>
class ContinuousSample {
public:
	explicit ContinuousSample( int sampleSize ) : sampleSize( sampleSize ), populationSize( 0 ), sorted( true ), _min(T()), _max(T()) {}

	ContinuousSample<T>& addSample(T sample) {
		if( !populationSize )
			_min = _max = sample;
		populationSize++;
		sorted = false;

		if( populationSize <= sampleSize ) {
			samples.push_back( sample );
		} else if( g_random->random01() < ( (double)sampleSize / populationSize ) ) {
			samples[ g_random->randomInt( 0, sampleSize ) ] = sample;
		}

		_max = std::max( _max, sample );
		_min = std::min( _min, sample );
		return *this;
	}

	double mean() {
		if (!samples.size()) return 0;
		T sum = 0;
		for( int c = 0; c < samples.size(); c++ )
			sum += samples[ c ];
		return (double)sum / samples.size();
	}

	T median() {
		return percentile( 0.5 );
	}

	T percentile( double percentile ) {
		if( !samples.size() || percentile < 0.0 || percentile > 1.0 )
			return T();
		sort();
		int idx = std::floor( ( samples.size() - 1 ) * percentile );
		return samples[ idx ];
	}

	T min() { return _min; }
	T max() { return _max; }

	void clear() {
		samples.clear();
		populationSize = 0;
		sorted = true;
		_min = _max = 0; // Doesn't work for all T
	}

private:
	int sampleSize;
	uint64_t populationSize;
	bool sorted;
	std::vector<T> samples;
	T _min, _max;

	void sort() {
		if( !sorted && samples.size() > 1 )
			std::sort( samples.begin(), samples.end() );
		sorted = true;
	}
};

#endif
