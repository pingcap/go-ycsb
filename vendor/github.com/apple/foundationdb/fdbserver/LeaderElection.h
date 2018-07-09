/*
 * LeaderElection.h
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

#ifndef FDBSERVER_LEADERELECTION_H
#define FDBSERVER_LEADERELECTION_H
#pragma once

#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"

class ServerCoordinators;

template <class LeaderInterface>
Future<Void> tryBecomeLeader( ServerCoordinators const& coordinators,
							  LeaderInterface const& proposedInterface,
							  Reference<AsyncVar<Optional<LeaderInterface>>> const& outKnownLeader,
							  bool hasConnected,
							  Reference<AsyncVar<ProcessClass>> const& asyncProcessClass,
							  Reference<AsyncVar<bool>> const& asyncIsExcluded);

// Participates in the given coordination group's leader election process, nominating the given
// LeaderInterface (presumed to be a local interface) as leader.  The leader election process is
// "sticky" - once a leader becomes leader, as long as its communications with other processes are
// good it will remain leader.  The outKnownLeader variable is updated to reflect a best guess of
// the current leader.  If the proposed interface becomes the leader, the outKnownLeader will be
// set to the proposedInterface, and then if it is displaced by another leader, the return value will
// eventually be set.  If the return value is cancelled, the candidacy or leadership of the proposedInterface
// will eventually end.

Future<Void> changeLeaderCoordinators( ServerCoordinators const& coordinators, Value const& forwardingInfo );
// Inform all the coordinators that they have been replaced with a new connection string

#pragma region Implementation

Future<Void> tryBecomeLeaderInternal( ServerCoordinators const& coordinators, Value const& proposedSerializedInterface, Reference<AsyncVar<Value>> const& outSerializedLeader, bool const& hasConnected, Reference<AsyncVar<ProcessClass>> const& asyncProcessClass, Reference<AsyncVar<bool>> const& asyncIsExcluded );

template <class LeaderInterface>
Future<Void> tryBecomeLeader( ServerCoordinators const& coordinators,
							  LeaderInterface const& proposedInterface,
							  Reference<AsyncVar<Optional<LeaderInterface>>> const& outKnownLeader,
							  bool hasConnected,
							  Reference<AsyncVar<ProcessClass>> const& asyncProcessClass,
							  Reference<AsyncVar<bool>> const& asyncIsExcluded)
{
	Reference<AsyncVar<Value>> serializedInfo( new AsyncVar<Value> );
	Future<Void> m = tryBecomeLeaderInternal( coordinators, BinaryWriter::toValue(proposedInterface, IncludeVersion()), serializedInfo, hasConnected, asyncProcessClass, asyncIsExcluded );
	return m || asyncDeserialize( serializedInfo, outKnownLeader );
}

#pragma endregion

#endif
