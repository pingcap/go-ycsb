/*
 * MasterInterface.h
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

#ifndef FDBSERVER_MASTERINTERFACE_H
#define FDBSERVER_MASTERINTERFACE_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/CommitTransaction.h"
#include "TLogInterface.h"

typedef uint64_t DBRecoveryCount;

struct MasterInterface {
	LocalityData locality;
	RequestStream< ReplyPromise<Void> > waitFailure;
	RequestStream< struct GetRateInfoRequest > getRateInfo;
	RequestStream< struct TLogRejoinRequest > tlogRejoin; // sent by tlog (whether or not rebooted) to communicate with a new master
	RequestStream< struct ChangeCoordinatorsRequest > changeCoordinators;
	RequestStream< struct GetCommitVersionRequest > getCommitVersion;

	NetworkAddress address() const { return changeCoordinators.getEndpoint().address; }

	UID id() const { return changeCoordinators.getEndpoint().token; }
	template <class Archive>
	void serialize(Archive& ar) {
		ASSERT( ar.protocolVersion() >= 0x0FDB00A200040001LL );
		ar & locality & waitFailure & getRateInfo & tlogRejoin & changeCoordinators & getCommitVersion;
	}

	void initEndpoints() {
		getCommitVersion.getEndpoint( TaskProxyGetConsistentReadVersion );
	}
};

struct GetRateInfoRequest {
	UID requesterID;
	int64_t totalReleasedTransactions;
	ReplyPromise<struct GetRateInfoReply> reply;

	GetRateInfoRequest() {}
	GetRateInfoRequest( UID const& requesterID, int64_t totalReleasedTransactions ) : requesterID(requesterID), totalReleasedTransactions(totalReleasedTransactions) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & requesterID & totalReleasedTransactions & reply;
	}
};

struct GetRateInfoReply {
	double transactionRate;
	double leaseDuration;

	template <class Ar>
	void serialize(Ar& ar) {
		ar & transactionRate & leaseDuration;
	}
};

struct TLogRejoinRequest {
	TLogInterface myInterface;
	DBRecoveryCount recoveryCount;
	ReplyPromise<bool> reply;   // false means someone else registered, so we should re-register.  true means this master is recovered, so don't send again to the same master.

	template <class Ar>
	void serialize(Ar& ar) {
		ar & myInterface & reply;
	}
};

struct ChangeCoordinatorsRequest {
	Standalone<StringRef> newConnectionString;
	ReplyPromise<Void> reply;  // normally throws even on success!

	ChangeCoordinatorsRequest() {}
	ChangeCoordinatorsRequest(Standalone<StringRef> newConnectionString) : newConnectionString(newConnectionString) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & newConnectionString & reply;
	}
};

struct ResolverMoveRef {
	KeyRangeRef range;
	int dest;

	ResolverMoveRef() : dest(0) {}
	ResolverMoveRef(KeyRangeRef const& range, int dest) : range(range), dest(dest) {}
	ResolverMoveRef( Arena& a, const ResolverMoveRef& copyFrom ) : range(a, copyFrom.range), dest(copyFrom.dest) {}

	bool operator == ( ResolverMoveRef const& rhs ) const {
		return range == rhs.range && dest == rhs.dest;
	}
	bool operator != ( ResolverMoveRef const& rhs ) const {
		return range != rhs.range || dest != rhs.dest;
	}

	size_t expectedSize() const {
		return range.expectedSize();
	}

	template <class Ar>
	void serialize( Ar& ar ) {
		ar & range & dest;
	}
};

struct GetCommitVersionReply {
	Standalone<VectorRef<ResolverMoveRef>> resolverChanges;
	Version resolverChangesVersion;
	Version version;
	Version prevVersion;
	uint64_t requestNum;

	GetCommitVersionReply() : resolverChangesVersion(0), version(0), prevVersion(0), requestNum(0) {}
	explicit GetCommitVersionReply( Version version, Version prevVersion, uint64_t requestNum ) : version(version), prevVersion(prevVersion), resolverChangesVersion(0), requestNum(requestNum) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & resolverChanges & resolverChangesVersion & version & prevVersion & requestNum;
	}
};

struct GetCommitVersionRequest {
	uint64_t requestNum;
	uint64_t mostRecentProcessedRequestNum;
	UID requestingProxy;
	ReplyPromise<GetCommitVersionReply> reply;

	GetCommitVersionRequest() { }
	GetCommitVersionRequest(uint64_t requestNum, uint64_t mostRecentProcessedRequestNum, UID requestingProxy)
		: requestNum(requestNum), mostRecentProcessedRequestNum(mostRecentProcessedRequestNum), requestingProxy(requestingProxy) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & requestNum & mostRecentProcessedRequestNum & requestingProxy & reply;
	}
};

struct LifetimeToken {
	UID ccID;
	int64_t count;

	LifetimeToken() : count(0) {}

	bool isStillValid( LifetimeToken const& latestToken, bool isLatestID ) const {
		return ccID == latestToken.ccID && (count >= latestToken.count || isLatestID);
	}
	std::string toString() const {
		return ccID.shortString() + format("#%lld", count);
	}
	void operator++() {
		++count;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & ccID & count;
	}
};

#endif