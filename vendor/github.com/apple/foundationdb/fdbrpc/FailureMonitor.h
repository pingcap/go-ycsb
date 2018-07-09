/*
 * FailureMonitor.h
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

#ifndef FLOW_FAILUREMONITOR_H
#define FLOW_FAILUREMONITOR_H
#pragma once

#include "flow/flow.h"
#include "flow/IndexedSet.h"
#include "FlowTransport.h" // Endpoint

using std::vector;

/*

IFailureMonitor is used by load balancing, data distribution and other components
to report on which other machines are unresponsive or experiencing other failures.
This is vital both to reconfigure the system in response to failures and to prevent
actors from waiting forever for replies from remote machines that are no longer
available.  When waiting for a reply, clients should generally stop waiting and
try an alternative server when a failure is reported, rather than relying on timeouts.

The information tracked for each machine is a FailureStatus, which
for the moment is just a boolean but might be richer in the future.

Get an IFailureMonitor by calling g_network->failureMonitor(); the simulator keeps
one for each simulated machine and ASIONetwork keeps one for each process.

The system attempts to ensure that failures are reported quickly, but may occasionally
report a working system as failed temporarily.  Clients that intend to take very costly
actions as a result of a failure should probably wait a while to see if a machine becomes
unfailed first.  If possible use onFailedFor() which in the future may react to 'permanent'
failures immediately.

The information reported through this interface is actually supplied by failureMonitorClient,
which exchanges FailureMonitoringRequest/Reply pairs with the failureDetectionServer actor on
the ClusterController.  This central repository of failure information has the opportunity
to take into account topology and global network conditions in identifying failures.  In
the future it may be augmented with locally available information about failures (e.g.
TCP connection loss in ASIONetwork or unexpectedly long response times for application requests).

Communications failures are tracked at NetworkAddress granularity.  When a request is made to
a missing endpoint on a non-failed machine, this information is reported back to the requesting
machine and tracked at the endpoint level.

*/

struct FailureStatus {
	bool failed;

	FailureStatus() : failed(true) {}
	explicit FailureStatus(bool failed) : failed(failed) {}
	bool isFailed() { return failed; }
	bool isAvailable() { return !failed; }

	bool operator == (FailureStatus const& r) const { return failed == r.failed; }
	bool operator != (FailureStatus const& r) const { return failed != r.failed; }
	template <class Ar>
	void serialize(Ar& ar) {
		ar & failed;
	}
};

class IFailureMonitor {
public:
	// Returns the currently known status for the endpoint
	virtual FailureStatus getState( Endpoint const& endpoint ) = 0;

	// Only use this function when the endpoint is known to be failed
	virtual void endpointNotFound( Endpoint const& ) = 0;

	// The next time the known status for the endpoint changes, returns the new status.
	virtual Future<Void> onStateChanged( Endpoint const& endpoint ) = 0;

	// Returns when onFailed(endpoint) || transport().onDisconnect( endpoint.address ), but more efficiently
	virtual Future<Void> onDisconnectOrFailure( Endpoint const& endpoint ) = 0;

	// Returns true if the endpoint is failed but the address of the endpoint is not failed.
	virtual bool onlyEndpointFailed( Endpoint const& endpoint ) = 0;

	// Returns true if the endpoint will never become available.
	virtual bool permanentlyFailed( Endpoint const& endpoint ) = 0;

	// Called by FlowTransport when a connection closes and a prior request or reply might be lost
	virtual void notifyDisconnect( NetworkAddress const& ) = 0;

	// Returns when the known status of endpoint is next equal to status.  Returns immediately
	//   if appropriate.
	Future<Void> onStateEqual( Endpoint const& endpoint, FailureStatus status );

	// Returns when the status of the given endpoint is next considered "failed"
	Future<Void> onFailed( Endpoint const& endpoint ) {
		return onStateEqual( endpoint, FailureStatus() );
	}

	static IFailureMonitor& failureMonitor() { return *static_cast<IFailureMonitor*>((void*) g_network->global(INetwork::enFailureMonitor)); }
	// Returns the failure monitor that the calling machine should use

	// Returns when the status of the given endpoint has continuously been "failed" for sustainedFailureDuration + (elapsedTime*sustainedFailureSlope)
	Future<Void> onFailedFor( Endpoint const& endpoint, double sustainedFailureDuration, double sustainedFailureSlope = 0.0 );
};

// SimpleFailureMonitor is the sole implementation of IFailureMonitor.  It has no
//   failure detection logic; it just implements the interface and reacts to setStatus() etc.
// Initially all addresses are considered failed, but all endpoints of a non-failed address are considered OK.
class SimpleFailureMonitor : public IFailureMonitor {
public:
	SimpleFailureMonitor() : endpointKnownFailed() { }
	void setStatus( NetworkAddress const& address, FailureStatus const& status );
	void endpointNotFound( Endpoint const& );
	virtual void notifyDisconnect( NetworkAddress const& );

	virtual Future<Void> onStateChanged( Endpoint const& endpoint );
	virtual FailureStatus getState( Endpoint const& endpoint );
	virtual Future<Void> onDisconnectOrFailure( Endpoint const& endpoint );
	virtual bool onlyEndpointFailed( Endpoint const& endpoint );
	virtual bool permanentlyFailed( Endpoint const& endpoint );

	void reset();
private:
	Map< NetworkAddress, FailureStatus > addressStatus;
	YieldedAsyncMap< Endpoint, bool > endpointKnownFailed;

	friend class OnStateChangedActorActor;
};

#endif