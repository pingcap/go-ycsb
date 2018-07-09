/*
 * ActorCollection.h
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

#ifndef FDBSERVER_ACTORCOLLECTION_H
#define FDBSERVER_ACTORCOLLECTION_H
#pragma once

#include "flow/flow.h"

// actorCollection
//   - Can add a future at any time
//   - Cancels all futures in deterministic order if cancelled
//   - Throws an error immediately if any future throws an error
//   - Never returns otherwise, unless returnWhenEmptied=true in which case returns the first time it goes from count 1 to count 0 futures
//   - Uses memory proportional to the number of unready futures added (i.e. memory
//     is freed promptly when an actor in the collection returns)
Future<Void> actorCollection( FutureStream<Future<Void>> const& addActor, int* const& optionalCountPtr = NULL, double* const& lastChangeTime = NULL, double* const& idleTime = NULL, double* const& allTime = NULL, bool const& returnWhenEmptied=false );

// ActorCollectionNoErrors is an easy-to-use wrapper for actorCollection() when you know that no errors will
// be thrown by the actors (e.g. because they are wrapped with individual error reporters).
struct ActorCollectionNoErrors : NonCopyable {
private:
	Future<Void> m_ac;
	PromiseStream<Future<Void>> m_add;
	int m_size;
	void init() { m_size = 0; m_ac = actorCollection(m_add.getFuture(), &m_size); }
public:
	ActorCollectionNoErrors() { init(); }
	void clear() { m_ac=Future<Void>(); init(); }
	void add( Future<Void> actor ) { m_add.send(actor); }
	int size() const { return m_size; }
};

// Easy-to-use wrapper that permits getting the result (error or returnWhenEmptied) from actorCollection
class ActorCollection : NonCopyable {
	PromiseStream<Future<Void>> m_add;
	Future<Void> m_out;

public:
	explicit ActorCollection( bool returnWhenEmptied ) {
		m_out = actorCollection(m_add.getFuture(), NULL, NULL, NULL, NULL, returnWhenEmptied );
	}

	void add( Future<Void> a ) { m_add.send(a); }
	Future<Void> getResult() { return m_out; }
	void clear( bool returnWhenEmptied ) { m_out.cancel(); m_out = actorCollection(m_add.getFuture(), NULL, NULL, NULL, NULL, returnWhenEmptied ); }
};

#endif