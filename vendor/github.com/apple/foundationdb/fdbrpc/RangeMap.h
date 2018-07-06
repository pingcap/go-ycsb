/*
 * RangeMap.h
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

#ifndef FLOW_RANGEMAP_H
#define FLOW_RANGEMAP_H
#pragma once

#include "flow/flow.h"
#include <boost/range.hpp>
#include "flow/IndexedSet.h"

using boost::iterator_range;

template <class Key>
class RangeMapRange {
public:
	Key begin, end;
	RangeMapRange( Key const& begin, Key const& end ) : begin(begin), end(end) {}
};

template <class Key>
RangeMapRange<Key> rangeMapRange( Key const& begin, Key const& end ) { return RangeMapRange<Key>(begin,end); }

template <class Metric>
struct ConstantMetric {
	template<typename pair_type>
	Metric operator()(pair_type const& p) const { return Metric(1); }
};

template <class Metric>
struct KeyBytesMetric {
	template<typename pair_type>
	Metric operator()(pair_type const& p) const { return Metric(p.key.size() + sizeof(pair_type)); }
};

template <class Metric>
struct ValueBytesMetric {
	template<typename pair_type>
	Metric operator()(pair_type const& p) const { return Metric(p.value.size() + sizeof(pair_type)); }
};

template <class Metric>
struct KeyValueBytesMetric {
	template<typename pair_type>
	Metric operator()(pair_type const& p) const { return Metric(p.key.size() + p.value.size() + sizeof(pair_type)); }
};

template <class Key, class Val, class Range = RangeMapRange<Key>, class Metric = int, class MetricFunc = ConstantMetric<Metric>>
class RangeMap {
private:
	typedef MapPair<Key,Val> pair_type;
public:
	//Applications may decrement an iterator before ranges begin, or increment after ranges end, but once in this state cannot do further incrementing or decrementing
	class Iterator {
	public:
		Iterator() {}	// singular
		Iterator( typename Map<Key,Val,pair_type,Metric>::iterator it ) : it(it) {}

		Key const& begin() { return it->key; }
		Key const& end() { auto j = it; ++j; return j->key; }

		Range range() { return Range(begin(),end()); }

		Val& value() { 
			//ASSERT( it->key != allKeys.end );
			return it->value; 
		}

		void operator ++() { ++it; }
		void operator --() { it.decrementNonEnd(); }
		bool operator ==(Iterator const& r) const { return it == r.it; }
		bool operator !=(Iterator const& r) const { return it != r.it; }
		
		// operator* and -> return this
		Iterator& operator*() { return *this; }
		Iterator* operator->() { return this; }

		typedef std::forward_iterator_tag iterator_category;
		typedef Iterator value_type;
		typedef int difference_type;
		typedef Iterator* pointer;
		typedef Iterator& reference;
	private:
		typename Map<Key,Val,pair_type,Metric>::iterator it;
	};
	typedef iterator_range<Iterator> Ranges;

	explicit RangeMap(Key endKey, Val v=Val(), MetricFunc m = MetricFunc()) : mf(m) { 
		Key beginKey = Key();
		pair_type beginPair(beginKey, v);
		map.insert(beginPair, true, mf(beginPair)); 
		pair_type endPair(endKey, Val());
		map.insert(endPair, true, mf(endPair)); 
	}
	Val const& operator[]( const Key& k ) { return rangeContaining(k).value(); }

	Ranges ranges() { return Ranges( Iterator(map.begin()), Iterator(map.lastItem()) ); }
	Ranges intersectingRanges( const Range& r ) { return Ranges(rangeContaining(r.begin), Iterator(map.lower_bound(r.end))); }
	// containedRanges() will return all ranges that are fully contained by the passed range (note that a range fully contains itself)
	Ranges containedRanges( const Range& r ) { 
		auto s = Iterator( map.lower_bound( r.begin ) );
		if ( s.begin() >= r.end ) return Ranges(s,s);
		return Ranges(s, rangeContaining(r.end));
	}
	template <class ComparableToKey>
	Iterator rangeContaining( const ComparableToKey& k ) { 
		return Iterator(map.lastLessOrEqual(k)); 
	}
	// Returns the range containing a key infinitesimally before k, or the first range if k==Key()
	template <class ComparableToKey>
	Iterator rangeContainingKeyBefore( const ComparableToKey& k ) {
		Iterator i = map.lower_bound(k);
		if ( !i->begin().size() ) return i;
		--i;
		return i;
	}
	Iterator lastItem() { 
		auto i = map.lastItem();
		i.decrementNonEnd();
		return Iterator(i); 
	}
	int size() const { return map.size() - 1; } // We always have one range bounded by two entries
	Iterator randomRange() {
		return Iterator( map.index( g_random->randomInt(0, map.size()-1) ) );
	}
	Iterator nthRange(int n) { return Iterator(map.index(n)); }

	bool allEqual( const Range& r, const Val& v );

	template <class ComparableToKey> 
	void coalesce( const ComparableToKey& k );
	void coalesce( const Range& k );
	void validateCoalesced();

	void operator=(RangeMap&& r) noexcept(true) { map = std::move(r.map); }
	//void clear( const Val& value ) { ranges.clear(); ranges.insert(std::make_pair(Key(),value)); }

	void insert( const Range& keys, const Val& value );

protected:
	Map<Key,Val,pair_type,Metric> map;
	const MetricFunc mf;
};

template <class Key, class Val, class Range, class Metric, class MetricFunc>
template <class ComparableToKey>
void RangeMap<Key,Val,Range,Metric,MetricFunc>::coalesce( const ComparableToKey& k ) {
	auto begin = map.lastLessOrEqual(k);
	auto end = begin;
	const Val& compareVal = begin->value;
	ASSERT( begin != map.end() );
	while( begin != map.begin() && begin->value == compareVal )
		begin.decrementNonEnd();
	while( end != map.lastItem() && end->value == compareVal )
		++end;
	if( begin->value != compareVal ) {
		++begin;
		if( begin == end ) return;
	}
	++begin;
	map.erase(begin, end);
}

template <class Key, class Val, class Range, class Metric, class MetricFunc>
void RangeMap<Key,Val,Range,Metric,MetricFunc>::coalesce( const Range& k ) {
	coalesce(k.begin);
	auto it = map.lastLessOrEqual(k.begin);
	Val* lastVal = &it->value;
	++it;
	if( it == map.end() )
		return;
	bool doCheck = true;
	while( it != map.lastItem() && doCheck ) {
		doCheck = it->key < k.end;
		if( it->value == *lastVal ) {
			doCheck = true;
			auto begin = it;
			++it;
			map.erase(begin, it);
		}
		else {
			lastVal = &it->value;
			++it;
		}
	}
	if( EXPENSIVE_VALIDATION )
		validateCoalesced();
}

template <class Key, class Val, class Range, class Metric, class MetricFunc>
void RangeMap<Key,Val,Range,Metric,MetricFunc>::validateCoalesced() {
	auto it = map.begin();
	Val* lastVal = &it->value;
	++it;
	auto end = map.lastItem();
	for( ; it != end; ++it ) {
		ASSERT( it->value != *lastVal );
		lastVal = &it->value;
	}
}

template <class Key, class Val, class Range, class Metric, class MetricFunc>
bool RangeMap<Key,Val,Range,Metric,MetricFunc>::allEqual( const Range& keys, const Val& val ) {
	auto r = intersectingRanges(keys);
	for(auto i=r.begin(); i!=r.end(); ++i)
		if (i.value() != val)
			return false;
	return true;
}

template <class Key, class Val, class Range, class Metric, class MetricFunc>
void RangeMap<Key,Val,Range,Metric,MetricFunc>::insert( const Range& keys, const Val& value ) {
	if(keys.begin == keys.end)
		return;

	auto end = map.lower_bound( keys.end );
	if( end->key != keys.end ) {
		end.decrementNonEnd();
		const Val& valueAfterRange = end->value;
		pair_type endPair(keys.end, valueAfterRange);
		end = map.insert(endPair, true, mf(endPair));
	}

	auto begin = map.lower_bound( keys.begin );
	
	map.erase(begin, end);
	pair_type beginPair(keys.begin, value);
	map.insert(beginPair, true, mf(beginPair));
}

#endif