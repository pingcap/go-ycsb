/*
 * BulkSetup.actor.h
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

#pragma once

// When actually compiled (NO_INTELLISENSE), include the generated
// version of this file.  In intellisense use the source version.
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_BULK_SETUP_ACTOR_G_H)
		#define FDBSERVER_BULK_SETUP_ACTOR_G_H
		#include "BulkSetup.actor.g.h"
#elif !defined(FDBSERVER_BULK_SETUP_ACTOR_H)
		#define FDBSERVER_BULK_SETUP_ACTOR_H

#include "flow/actorcompiler.h"
#include "fdbclient/NativeAPI.h"
#include "workloads.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbrpc/simulator.h"

#include <string>

ACTOR template<class T>
Future<bool> checkRangeSimpleValueSize( Database cx, T* workload, uint64_t begin, uint64_t end) {
	loop {
		state Transaction tr(cx);
		try {
			state Standalone< KeyValueRef > firstKV = (*workload)( begin );
			state Standalone< KeyValueRef > lastKV = (*workload)( end - 1 );
			state Future< Optional< Value > > first = tr.get( firstKV.key );
			state Future< Optional< Value > > last = tr.get( lastKV.key );
			Void _ = wait( success( first ) && success( last ) );
			return first.get().present() && last.get().present();
		} catch (Error& e) {
			TraceEvent("CheckRangeError").detail("Begin", begin).detail("End", end).error(e);
			Void _ = wait( tr.onError(e) );
		}
	}
}

// Returns true if the range was added
ACTOR template<class T>
Future<uint64_t> setupRange( Database cx, T* workload, uint64_t begin, uint64_t end) {
	state uint64_t bytesInserted = 0;
	loop {
		state Transaction tr(cx);
		try {
			//if( g_random->random01() < 0.001 )
			//	tr.debugTransaction( g_random->randomUniqueID() );

			state Standalone<KeyValueRef> sampleKV = (*workload)( begin );
			Optional<Value> f = wait( tr.get( sampleKV.key ) );
			if (f.present()) {
//				if( sampleKV.value.size() == f.get().size() ) {
					//TraceEvent("BulkSetupRangeAlreadyPresent")
					//	.detail("Begin", begin)
					//	.detail("End", end)
					//	.detail("Version", tr.getReadVersion().get())
					//	.detail("Key", printable((*workload)(begin).key))
					//	.detailf("From", "%016llx", debug_lastLoadBalanceResultEndpointToken);
					return bytesInserted;  // The transaction already completed!
				//}
				//TraceEvent(SevError, "BulkSetupConflict")
				//	.detail("Begin", begin)
				//	.detail("End", end)
				//	.detail("ExpectedLength", sampleKV.value.size())
				//	.detail("FoundLength", f.get().size());
				//throw operation_failed();
			}
			// Predefine a single large write conflict range over the whole key space
			tr.addWriteConflictRange( KeyRangeRef(
					workload->keyForIndex( begin ),
					keyAfter( workload->keyForIndex( end ) ) ) );
			bytesInserted = 0;
			for(uint64_t n=begin; n<end; n++) {
				Standalone<KeyValueRef> kv = (*workload)( n );
				tr.set( kv.key, kv.value );
				bytesInserted += kv.key.size() + kv.value.size();
			}
			Void _ = wait( tr.commit() );
			return bytesInserted;
		} catch (Error& e) {
			Void _ = wait( tr.onError(e) );
		}
	}
}

ACTOR template<class T>
Future<uint64_t> setupRangeWorker( Database cx, T* workload, vector<std::pair<uint64_t,uint64_t>>* jobs, double maxKeyInsertRate, int keySaveIncrement, int actorId) {
	state double nextStart;
	state uint64_t loadedRanges = 0;
	state int lastStoredKeysLoaded = 0;
	state uint64_t keysLoaded = 0;
	state uint64_t bytesStored = 0;
	while (jobs->size()){
		state pair<uint64_t, uint64_t> job = jobs->back();
		jobs->pop_back();
		nextStart = now() + (job.second-job.first)/maxKeyInsertRate;
		uint64_t numBytes = wait( setupRange(cx, workload, job.first, job.second) );
		if( numBytes > 0 )
			loadedRanges++;

		if(keySaveIncrement > 0)
		{
			keysLoaded += job.second - job.first;
			bytesStored += numBytes;

			if(keysLoaded - lastStoredKeysLoaded >= keySaveIncrement || jobs->size() == 0)
			{
				state Transaction tr(cx);
				try
				{
					std::string countKey = format("keycount|%d|%d", workload->clientId, actorId);
					std::string bytesKey = format("bytesstored|%d|%d", workload->clientId, actorId);

					tr.set(StringRef(countKey), StringRef((uint8_t*)&keysLoaded, sizeof(uint64_t)));
					tr.set(StringRef(bytesKey), StringRef((uint8_t*)&bytesStored, sizeof(uint64_t)));

					Void _ = wait(tr.commit());
					lastStoredKeysLoaded = keysLoaded;
				}
				catch(Error& e)
				{
					Void _ = wait(tr.onError(e));
				}
			}
		}

		if (now() < nextStart)
			Void _ = wait( delayUntil(nextStart));
	}
	return loadedRanges;
}

//Periodically determines how many keys have been inserted.  If the count has just exceeded a count of interest, computes the time taken
//to reach that mark.  Returns a vector of times (in seconds) corresponding to the counts in the countsOfInterest vector.

//Expects countsOfInterest to be sorted in ascending order
ACTOR static Future<vector<pair<uint64_t, double> > > trackInsertionCount(Database cx, vector<uint64_t> countsOfInterest, double checkInterval)
{
	state KeyRange keyPrefix = KeyRangeRef(std::string("keycount"), std::string("keycount") + char(255));
	state KeyRange bytesPrefix = KeyRangeRef(std::string("bytesstored"), std::string("bytesstored") + char(255));
	state Transaction tr(cx);
	state uint64_t lastInsertionCount = 0;
	state int currentCountIndex = 0;

	state vector<pair<uint64_t, double> > countInsertionRates;

	state double startTime = now();

	while(currentCountIndex < countsOfInterest.size())
	{
		try
		{
			state Future<Standalone<RangeResultRef>> countFuture = tr.getRange(keyPrefix, 1000000000);
			state Future<Standalone<RangeResultRef>> bytesFuture = tr.getRange(bytesPrefix, 1000000000);
			Void __ = wait(success(countFuture) && success(bytesFuture));

			Standalone<RangeResultRef> counts = countFuture.get();
			Standalone<RangeResultRef> bytes = bytesFuture.get();

			uint64_t numInserted = 0;
			for(int i = 0; i < counts.size(); i++)
				numInserted += *(uint64_t*)counts[i].value.begin();

			uint64_t bytesInserted = 0;
			for(int i = 0; i < bytes.size(); i++)
				bytesInserted += *(uint64_t*)bytes[i].value.begin();

			while(currentCountIndex < countsOfInterest.size() && countsOfInterest[currentCountIndex] > lastInsertionCount && countsOfInterest[currentCountIndex] <= numInserted)
				countInsertionRates.push_back(pair<uint64_t, double>(countsOfInterest[currentCountIndex++], bytesInserted / (now() - startTime)));

			lastInsertionCount = numInserted;
			Void _ = wait(delay(checkInterval));
		}
		catch(Error& e)
		{
			Void _ = wait(tr.onError(e));
		}
	}

	return countInsertionRates;
}

ACTOR template<class T>
Future<Void> bulkSetup( Database cx, T* workload, uint64_t nodeCount, Promise<double> setupTime,
		bool valuesInconsequential = false, double postSetupWarming = 0.0, double maxKeyInsertRate = 1e12,
		vector<uint64_t> insertionCountsToMeasure = vector<uint64_t>(), Promise<vector<pair<uint64_t, double> > > ratesAtKeyCounts = Promise<vector<pair<uint64_t, double> > >(),
		int keySaveIncrement = 0, double keyCheckInterval = 0.1 ) {

	state vector<pair<uint64_t,uint64_t>> jobs;
	state Future<Void> disabler = disableConnectionFailuresAfter(300, "BulkSetup");
	state uint64_t startNode = (nodeCount * workload->clientId) / workload->clientCount;
	state uint64_t endNode = (nodeCount * (workload->clientId+1)) / workload->clientCount;

	state double start = now();

	TraceEvent("BulkSetupStart")
		.detail("NodeCount", nodeCount)
		.detail("ValuesInconsequential", valuesInconsequential)
		.detail("PostSetupWarming", postSetupWarming)
		.detail("MaxKeyInsertRate", maxKeyInsertRate);

	// For bulk data schemes where the value of the key is not critical to operation, check to
	//  see if the database has already been set up.
	if( valuesInconsequential ) {
		bool present = wait( checkRangeSimpleValueSize( cx, workload, startNode, endNode ) );
		if( present ) {
			TraceEvent("BulkSetupRangeAlreadyPresent")
				.detail("Begin", startNode)
				.detail("End", endNode)
				.detail("CheckMethod", "SimpleValueSize");
			setupTime.send(0.0);
			ratesAtKeyCounts.send(vector<pair<uint64_t, double> >());
			return Void();
		} else {
			TraceEvent("BulkRangeNotFound")
				.detail("Begin", startNode)
				.detail("End", endNode)
				.detail("CheckMethod", "SimpleValueSize");
		}
	}

	Void _ = wait( delay( g_random->random01() / 4 ) );  // smear over .25 seconds

	state int BULK_SETUP_WORKERS = 40;
	// See that each chunk inserted is about 10KB
	int size_total = 0;
	for( int i = 0; i < 100; i++ ) {
		Standalone<KeyValueRef> sampleKV = (*workload)( startNode + (uint64_t)(g_random->random01()*(endNode - startNode)) );
		size_total += sampleKV.key.size() + sampleKV.value.size();
	}
	state int BULK_SETUP_RANGE_SIZE = size_total == 0 ? 50 : std::max(1, 10000 / (size_total / 100));

	TraceEvent((workload->description() + "SetupStart").c_str())
		.detail("ClientIdx", workload->clientId)
		.detail("ClientCount", workload->clientCount)
		.detail("StartingNode", startNode)
		.detail("NodesAssigned", endNode-startNode)
		.detail("NodesPerInsertion", BULK_SETUP_RANGE_SIZE)
		.detail("SetupActors", BULK_SETUP_WORKERS);

	// create a random vector of range-create jobs
	for(uint64_t n=startNode; n<endNode; n+=BULK_SETUP_RANGE_SIZE)
		jobs.push_back( std::make_pair( n, std::min(endNode, n+BULK_SETUP_RANGE_SIZE) ) );
	g_random->randomShuffle(jobs);

	// fire up the workers and wait for them to eat all the jobs
	double maxWorkerInsertRate = maxKeyInsertRate / BULK_SETUP_WORKERS / workload->clientCount;
	state vector<Future<uint64_t>> fs;

	state Future<vector<pair<uint64_t, double> > > insertionTimes = vector<pair<uint64_t, double> >();

	if(insertionCountsToMeasure.size() > 0)
	{
		std::sort(insertionCountsToMeasure.begin(), insertionCountsToMeasure.end());
		for(int i = 0; i < insertionCountsToMeasure.size(); i++)
			if(insertionCountsToMeasure[i] > nodeCount)
				insertionCountsToMeasure.erase(insertionCountsToMeasure.begin() + i, insertionCountsToMeasure.end());

		if(workload->clientId == 0)
			insertionTimes = trackInsertionCount(cx, insertionCountsToMeasure, keyCheckInterval);

		if(keySaveIncrement <= 0)
			keySaveIncrement = BULK_SETUP_RANGE_SIZE;
	}
	else
		keySaveIncrement = 0;

	for(int j=0; j<BULK_SETUP_WORKERS; j++)
		fs.push_back( setupRangeWorker(cx, workload, &jobs, maxWorkerInsertRate, keySaveIncrement, j) );
	try {
		Void _ = wait( success(insertionTimes) && waitForAll(fs) );
	} catch(Error& e) {
		if( e.code() == error_code_operation_failed ) {
			TraceEvent(SevError, "BulkSetupFailed").error(e);
		}
		throw;
	}

	ratesAtKeyCounts.send(insertionTimes.get());

	uint64_t rangesInserted = 0;
	for(int i = 0; i < fs.size(); i++)
		rangesInserted += fs[i].get();

	state double elapsed = now() - start;
	setupTime.send( elapsed );
	TraceEvent("SetupLoadComplete")
		.detail("LoadedRanges", rangesInserted)
		.detail("Duration", elapsed)
		.detail("Nodes", nodeCount);

	// Here we wait for data in flight to go to 0 (this will not work on a database with other users)
	if( postSetupWarming != 0 ) {
		try {
			Void _ = wait( delay( 5.0 ) );  // Wait for the data distribution in a small test to start
			loop {
				int64_t inFlight = wait( getDataInFlight( cx, workload->dbInfo ) );
				TraceEvent("DynamicWarming").detail("inFlight", inFlight);
				if( inFlight > 1e6 ) {  // Wait for just 1 MB to be in flight
					Void _ = wait( delay( 1.0 ) );
				} else {
					Void _ = wait( delay( 1.0 ) );
					TraceEvent("DynamicWarmingDone");
					break;
				}
			}
		} catch (Error& e) {
			if( e.code() == error_code_actor_cancelled )
				throw;
			TraceEvent("DynamicWarmingError").error(e);
			if( postSetupWarming > 0 )
				Void _ = wait( timeout( databaseWarmer( cx ), postSetupWarming, Void() ) );
		}
	}

	TraceEvent((workload->description() + "SetupOK").c_str())
		.detail("ClientIdx", workload->clientId)
		.detail("WarmingDelay", postSetupWarming)
		.detail("KeyLoadElapsedTime", elapsed);
	return Void();
}

#endif
