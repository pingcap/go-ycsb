/*
 * StorageMetrics.actor.h
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

// Included via StorageMetrics.h
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/simulator.h"
#include "flow/UnitTest.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/KeyRangeMap.h"
#include "Knobs.h"

struct StorageMetricSample {
	IndexedSet<Key, int64_t> sample;
	int64_t metricUnitsPerSample;

	StorageMetricSample( int64_t metricUnitsPerSample ) : metricUnitsPerSample(metricUnitsPerSample) {}

	int64_t getEstimate( KeyRangeRef keys ) const {
		return sample.sumRange( keys.begin, keys.end );
	}
	KeyRef splitEstimate( KeyRangeRef range, int64_t offset, bool front = true ) const {
		auto fwd_split = sample.index( front ? sample.sumTo(sample.lower_bound(range.begin)) + offset : sample.sumTo(sample.lower_bound(range.end)) - offset );
		
		if( fwd_split == sample.end() || *fwd_split >= range.end )
			return range.end;

		if( !front && *fwd_split <= range.begin )
			return range.begin;

		auto bck_split = fwd_split;

		// Butterfly search - start at midpoint then go in both directions.
		while ((fwd_split != sample.end() && *fwd_split < range.end) ||
			   (bck_split != sample.begin() && *bck_split > range.begin)) {
			if (bck_split != sample.begin() && *bck_split > range.begin) {
				auto it = bck_split;
				bck_split.decrementNonEnd();

				KeyRef split = keyBetween(KeyRangeRef(bck_split != sample.begin() ? std::max<KeyRef>(*bck_split,range.begin) : range.begin, *it));
				if(!front || (getEstimate(KeyRangeRef(range.begin, split)) > 0 && split.size() <= CLIENT_KNOBS->SPLIT_KEY_SIZE_LIMIT))
					return split;
			}

			if (fwd_split != sample.end() && *fwd_split < range.end) {
				auto it = fwd_split;
				++it;

				KeyRef split = keyBetween(KeyRangeRef(*fwd_split, it != sample.end() ? std::min<KeyRef>(*it, range.end) : range.end));
				if(front || (getEstimate(KeyRangeRef(split, range.end)) > 0 && split.size() <= CLIENT_KNOBS->SPLIT_KEY_SIZE_LIMIT))
					return split;

				fwd_split = it;
			}

		}

		// If we didn't return above, we didn't find anything.
		TraceEvent(SevWarnAlways, "CannotSplitLastSampleKey").detail("range", printable(range)).detail("offset", offset);
		return front ? range.end : range.begin;
	}
};

TEST_CASE("fdbserver/StorageMetricSample/simple") {
	StorageMetricSample s( 1000 );
	s.sample.insert(LiteralStringRef("Apple"), 1000);
	s.sample.insert(LiteralStringRef("Banana"), 2000);
	s.sample.insert(LiteralStringRef("Cat"), 1000);
	s.sample.insert(LiteralStringRef("Cathode"), 1000);
	s.sample.insert(LiteralStringRef("Dog"), 1000);

	ASSERT(s.getEstimate(KeyRangeRef(LiteralStringRef("A"), LiteralStringRef("D"))) == 5000);
	ASSERT(s.getEstimate(KeyRangeRef(LiteralStringRef("A"), LiteralStringRef("E"))) == 6000);
	ASSERT(s.getEstimate(KeyRangeRef(LiteralStringRef("B"), LiteralStringRef("C"))) == 2000);

	//ASSERT(s.splitEstimate(KeyRangeRef(LiteralStringRef("A"), LiteralStringRef("D")), 3500) == LiteralStringRef("Cat"));

	return Void();
}

struct TransientStorageMetricSample : StorageMetricSample {
	Deque< std::pair<double, std::pair<Key, int64_t>> > queue;

	TransientStorageMetricSample( int64_t metricUnitsPerSample ) : StorageMetricSample(metricUnitsPerSample) {}

	bool roll( KeyRef key, int64_t metric ) {
		return g_random->random01() < (double)metric / metricUnitsPerSample;	//< SOMEDAY: Better randomInt64?
	}

	// Returns the sampled metric value (possibly 0, possibly increased by the sampling factor)
	int64_t addAndExpire( KeyRef key, int64_t metric, double expiration ) {
		int64_t x = add( key, metric );
		if (x)
			queue.push_back( std::make_pair( expiration, std::make_pair( *sample.find(key), -x ) ) );
		return x;
	}

	//FIXME: both versions of erase are broken, because they do not remove items in the queue with will subtract a metric from the value sometime in the future
	int64_t erase( KeyRef key ) {
		auto it = sample.find(key);
		if (it == sample.end()) return 0;
		int64_t x = sample.getMetric(it);
		sample.erase(it);
		return x;
	}
	void erase( KeyRangeRef keys ) {
		sample.erase( keys.begin, keys.end );
	}

	void poll(KeyRangeMap< vector< PromiseStream< StorageMetrics > > > & waitMap, StorageMetrics m) {
		double now = ::now();
		while (queue.size() && 
				queue.front().first <= now )
		{
			KeyRef key = queue.front().second.first;
			int64_t delta = queue.front().second.second;
			ASSERT( delta != 0 );

			if( sample.addMetric( key, delta ) == 0 )
				sample.erase( key );

			StorageMetrics deltaM = m * delta;
			auto v = waitMap[key];
			for(int i=0; i<v.size(); i++) {
				TEST( true ); // TransientStorageMetricSample poll update 
				v[i].send( deltaM );
			}

			queue.pop_front();
		}
	}

	void poll() {
		double now = ::now();
		while (queue.size() && 
				queue.front().first <= now )
		{
			KeyRef key = queue.front().second.first;
			int64_t delta = queue.front().second.second;
			ASSERT( delta != 0 );

			if( sample.addMetric( key, delta ) == 0 )
				sample.erase( key );

			queue.pop_front();
		}
	}

private:
	int64_t add( KeyRef key, int64_t metric ) {
		if (!metric) return 0;
		int64_t mag = metric<0 ? -metric : metric;

		if (mag < metricUnitsPerSample) {
			if ( !roll(key, mag) )
				return 0;
			metric = metric<0 ? -metricUnitsPerSample : metricUnitsPerSample;
		}
		
		if( sample.addMetric( key, metric ) == 0 )
			sample.erase( key );

		return metric;
	}
};

struct StorageServerMetrics {
	KeyRangeMap< vector< PromiseStream< StorageMetrics > > > waitMetricsMap;
	StorageMetricSample byteSample;
	TransientStorageMetricSample iopsSample, bandwidthSample;	// FIXME: iops and bandwidth calculations are not effectively tested, since they aren't currently used by data distribution

	StorageServerMetrics()
		: byteSample( 0 ), iopsSample( SERVER_KNOBS->IOPS_UNITS_PER_SAMPLE ), bandwidthSample( SERVER_KNOBS->BANDWIDTH_UNITS_PER_SAMPLE )
	{
	}

	// Get the current estimated metrics for the given keys
	StorageMetrics getMetrics( KeyRangeRef const& keys ) {
		StorageMetrics result;
		result.bytes = byteSample.getEstimate( keys );
		result.bytesPerKSecond = bandwidthSample.getEstimate( keys ) * SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
		result.iosPerKSecond = iopsSample.getEstimate( keys ) * SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
		return result;
	}

	// Called when metrics should change (IO for a given key)
	// Notifies waiting WaitMetricsRequests through waitMetricsMap, and updates metricsAverageQueue and metricsSampleMap
	void notify( KeyRef key, StorageMetrics& metrics ) {
		ASSERT (metrics.bytes == 0); // ShardNotifyMetrics
		TEST (metrics.bytesPerKSecond != 0); // ShardNotifyMetrics
		TEST (metrics.iosPerKSecond != 0); // ShardNotifyMetrics

		double expire = now() + SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL;

		StorageMetrics notifyMetrics;

		if (metrics.bytesPerKSecond)
			notifyMetrics.bytesPerKSecond = bandwidthSample.addAndExpire( key, metrics.bytesPerKSecond, expire ) * SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
		if (metrics.iosPerKSecond)
			notifyMetrics.iosPerKSecond = iopsSample.addAndExpire( key, metrics.iosPerKSecond, expire ) * SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
		if (!notifyMetrics.allZero()) {
			auto& v = waitMetricsMap[key];
			for(int i=0; i<v.size(); i++) {
				TEST( true ); // ShardNotifyMetrics
				v[i].send( notifyMetrics );
			}
		}
	}

	// Called by StorageServerDisk when the size of a key in byteSample changes, to notify WaitMetricsRequest
	// Should not be called for keys past allKeys.end
	void notifyBytes( RangeMap<Key, std::vector<PromiseStream<StorageMetrics>>, KeyRangeRef>::Iterator shard, int64_t bytes ) {
		ASSERT(shard.end() <= allKeys.end);

		StorageMetrics notifyMetrics;
		notifyMetrics.bytes = bytes;
		for(int i=0; i < shard.value().size(); i++) {
			TEST( true ); // notifyBytes
			shard.value()[i].send( notifyMetrics );
		}
	}

	// Called by StorageServerDisk when the size of a key in byteSample changes, to notify WaitMetricsRequest
	void notifyBytes( KeyRef key, int64_t bytes ) {
		if( key >= allKeys.end ) //Do not notify on changes to internal storage server state
			return;

		notifyBytes(waitMetricsMap.rangeContaining(key), bytes);
	}

	// Called when a range of keys becomes unassigned (and therefore not readable), to notify waiting WaitMetricsRequests (also other types of wait
	//   requests in the future?)
	void notifyNotReadable( KeyRangeRef keys ) {
		auto rs = waitMetricsMap.intersectingRanges(keys);
		for (auto r = rs.begin(); r != rs.end(); ++r){
			auto &v = r->value();
			TEST( v.size() );  // notifyNotReadable() sending errors to intersecting ranges
			for (int n=0; n<v.size(); n++)
				v[n].sendError( wrong_shard_server() );
		}
	}

	// Called periodically (~1 sec intervals) to remove older IOs from the averages
	// Removes old entries from metricsAverageQueue, updates metricsSampleMap accordingly, and notifies
	//   WaitMetricsRequests through waitMetricsMap.
	void poll() {
		{ StorageMetrics m; m.bytesPerKSecond = SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS; bandwidthSample.poll(waitMetricsMap, m); }
		{ StorageMetrics m; m.iosPerKSecond = SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS; iopsSample.poll(waitMetricsMap, m); }
		// bytesSample doesn't need polling because we never call addExpire() on it
	}

	//static void waitMetrics( StorageServerMetrics* const& self, WaitMetricsRequest const& req );

	// This function can run on untrusted user data.  We must validate all divisions carefully.
	KeyRef getSplitKey( int64_t remaining, int64_t estimated, int64_t limits, int64_t used, int64_t infinity,
		bool isLastShard, StorageMetricSample& sample, double divisor, KeyRef const& lastKey, KeyRef const& key, bool hasUsed ) 
	{	
		ASSERT(remaining >= 0);
		ASSERT(limits > 0);
		ASSERT(divisor > 0);

		if( limits < infinity / 2 ) {
			int64_t expectedSize;
			if( isLastShard || remaining > estimated ) {
				double remaining_divisor = ( double( remaining ) / limits ) + 0.5;
				expectedSize = remaining / remaining_divisor;
			} else {
				// If we are here, then estimated >= remaining >= 0
				double estimated_divisor = ( double( estimated ) / limits ) + 0.5;
				expectedSize = remaining / estimated_divisor;
			}

			if( remaining > expectedSize ) {
				// This does the conversion from native units to bytes using the divisor.
				double offset = (expectedSize - used) / divisor;
				if( offset <= 0 )
					return hasUsed ? lastKey : key;
				return sample.splitEstimate( KeyRangeRef(lastKey, key), offset * ( ( 1.0 - SERVER_KNOBS->SPLIT_JITTER_AMOUNT ) + 2 * g_random->random01() * SERVER_KNOBS->SPLIT_JITTER_AMOUNT ) );
			}
		}

		return key;
	}

	void splitMetrics( SplitMetricsRequest req ) {
		try {
			SplitMetricsReply reply;
			KeyRef lastKey = req.keys.begin;
			StorageMetrics used = req.used;
			StorageMetrics estimated = req.estimated;
			StorageMetrics remaining = getMetrics( req.keys ) + used;

			//TraceEvent("SplitMetrics").detail("Begin", printable(req.keys.begin)).detail("End", printable(req.keys.end)).detail("Remaining", remaining.bytes).detail("Used", used.bytes);
			
			while( true ) {
				if( remaining.bytes < 2*SERVER_KNOBS->MIN_SHARD_BYTES )
					break;
				KeyRef key = req.keys.end;
				bool hasUsed = used.bytes != 0 || used.bytesPerKSecond != 0 || used.iosPerKSecond != 0;
				key = getSplitKey( remaining.bytes, estimated.bytes, req.limits.bytes, used.bytes, 
					req.limits.infinity, req.isLastShard, byteSample, 1, lastKey, key, hasUsed );
				if( used.bytes < SERVER_KNOBS->MIN_SHARD_BYTES )
					key = std::max( key, byteSample.splitEstimate( KeyRangeRef(lastKey, req.keys.end), SERVER_KNOBS->MIN_SHARD_BYTES - used.bytes ) );
				key = getSplitKey( remaining.iosPerKSecond, estimated.iosPerKSecond, req.limits.iosPerKSecond, used.iosPerKSecond, 
					req.limits.infinity, req.isLastShard, iopsSample, SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS, lastKey, key, hasUsed );
				key = getSplitKey( remaining.bytesPerKSecond, estimated.bytesPerKSecond, req.limits.bytesPerKSecond, used.bytesPerKSecond, 
					req.limits.infinity, req.isLastShard, bandwidthSample, SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS, lastKey, key, hasUsed );
				ASSERT( key != lastKey || hasUsed);
				if( key == req.keys.end )
					break;
				reply.splits.push_back_deep( reply.splits.arena(), key );

				StorageMetrics diff = (getMetrics( KeyRangeRef(lastKey, key) ) + used);
				remaining -= diff;
				estimated -= diff;
				
				used = StorageMetrics();
				lastKey = key;
			}

			reply.used = getMetrics( KeyRangeRef(lastKey, req.keys.end) ) + used;
			req.reply.send(reply);
		} catch (Error& e) {
			req.reply.sendError(e);
		}
	}

	void getPhysicalMetrics( GetPhysicalMetricsRequest req, StorageBytes sb ){
		GetPhysicalMetricsReply rep;

		// SOMEDAY: make bytes dynamic with hard disk space
		rep.load = getMetrics(allKeys);

		if (sb.free < 1e9 && g_random->random01() < 0.1)
			TraceEvent(SevWarn, "PhysicalDiskMetrics")
				.detail("free", sb.free)
				.detail("total", sb.total)
				.detail("available", sb.available)
				.detail("load", rep.load.bytes);

		rep.free.bytes = sb.free;
		rep.free.iosPerKSecond = 10e6;
		rep.free.bytesPerKSecond = 100e9;

		rep.capacity.bytes = sb.total;
		rep.capacity.iosPerKSecond = 10e6;
		rep.capacity.bytesPerKSecond = 100e9;

		req.reply.send(rep);
	}

	Future<Void> waitMetrics(WaitMetricsRequest req, Future<Void> delay);

private:
	static void collapse( KeyRangeMap<int>& map, KeyRef const& key ) {
		auto range = map.rangeContaining(key);
		if (range == map.ranges().begin() || range == map.ranges().end()) return;
		int value = range->value();
		auto prev = range; --prev;
		if (prev->value() != value) return;
		KeyRange keys = KeyRangeRef( prev->begin(), range->end() );
		map.insert( keys, value );
	}

	static void add( KeyRangeMap<int>& map, KeyRangeRef const& keys, int delta ) {
		auto rs = map.modify(keys);
		for(auto r = rs.begin(); r != rs.end(); ++r)
			r->value() += delta;
		collapse( map, keys.begin );
		collapse( map, keys.end );
	}
};

//Contains information about whether or not a key-value pair should be included in a byte sample
//Also contains size information about the byte sample
struct ByteSampleInfo {
	bool inSample;

	//Actual size of the key value pair
	int64_t size;

	//The recorded size of the sample (max of bytesPerSample, size)
	int64_t sampledSize;
};

//Determines whether a key-value pair should be included in a byte sample
//Also returns size information about the sample
ByteSampleInfo isKeyValueInSample(KeyValueRef keyValue);
