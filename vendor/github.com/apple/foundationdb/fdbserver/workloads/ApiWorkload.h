/*
 * ApiWorkload.h
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

#ifndef FDBSERVER_APIWORKLOAD_H
#define FDBSERVER_APIWORKLOAD_H
#pragma once

#include "flow/actorcompiler.h"
#include "workloads.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/ThreadSafeTransaction.h"

#include "MemoryKeyValueStore.h"

//an enumeration of apis being tested
enum TransactionType
{
	NATIVE,
	READ_YOUR_WRITES,
	THREAD_SAFE,
	MULTI_VERSION
};

//A wrapper interface for dealing with different Transaction implementations
struct TransactionWrapper : public ReferenceCounted<TransactionWrapper> {

	virtual ~TransactionWrapper() { }

	//Sets a key-value pair in the database
	virtual void set(KeyRef &key, ValueRef &value) = 0;

	//Commits modifications to the database
	virtual Future<Void> commit() = 0;

	//Gets a value associated with a given key from the database
	virtual Future<Optional<Value>> get(KeyRef &key) = 0;

	//Gets a range of key-value pairs from the database specified by a key range
	virtual Future<Standalone<RangeResultRef>> getRange(KeyRangeRef &keys, int limit, bool reverse) = 0;

	//Gets a range of key-value pairs from the database specified by a pair of key selectors
	virtual Future<Standalone<RangeResultRef>> getRange(KeySelectorRef &begin, KeySelectorRef &end, int limit, bool reverse) = 0;

	//Gets the key from the database specified by a given key selector
	virtual Future<Key> getKey(KeySelectorRef &key) = 0;

	//Clears a key from the database
	virtual void clear(KeyRef &key) = 0;

	//Clears a range of keys from the database
	virtual void clear(KeyRangeRef &range) = 0;

	//Processes transaction error conditions
	virtual Future<Void> onError(Error const& e) = 0;

	//Gets the read version of a transaction
	virtual Future<Version> getReadVersion() = 0;

	//Gets the committed version of a transaction
	virtual Version getCommittedVersion() = 0;

	//Prints debugging messages for a transaction; not implemented for all transaction types
	virtual void debugTransaction(UID debugId) {}

	virtual void addReadConflictRange( KeyRangeRef const& keys ) = 0;
};

//A wrapper class for flow based transactions (NativeAPI, ReadYourWrites)
template<class T>
struct FlowTransactionWrapper : public TransactionWrapper {
	Database cx;
	Database extraDB;
	bool useExtraDB;
	T transaction;
	T lastTransaction;
	FlowTransactionWrapper(Database cx, Database extraDB, bool useExtraDB) : cx(cx), extraDB(extraDB), useExtraDB(useExtraDB), transaction(cx) {
		if(useExtraDB && g_random->random01() < 0.5) {
			transaction = T(extraDB);
		}
	}
	virtual ~FlowTransactionWrapper() { }

	//Sets a key-value pair in the database
	void set(KeyRef &key, ValueRef &value) {
		transaction.set(key, value);
	}

	//Commits modifications to the database
	Future<Void> commit() {
		return transaction.commit();
	}

	//Gets a value associated with a given key from the database
	Future<Optional<Value>> get(KeyRef &key) {
		return transaction.get(key);
	}

	//Gets a range of key-value pairs from the database specified by a key range
	Future<Standalone<RangeResultRef>> getRange(KeyRangeRef &keys, int limit, bool reverse) {
		return transaction.getRange(keys, limit, false, reverse);
	}

	//Gets a range of key-value pairs from the database specified by a pair of key selectors
	Future<Standalone<RangeResultRef>> getRange(KeySelectorRef &begin, KeySelectorRef &end, int limit, bool reverse) {
		return transaction.getRange(begin, end, limit, false, reverse);
	}

	//Gets the key from the database specified by a given key selector
	Future<Key> getKey(KeySelectorRef &key) {
		return transaction.getKey(key);
	}

	//Clears a key from the database
	void clear(KeyRef &key) {
		transaction.clear(key);
	}

	//Clears a range of keys from the database
	void clear(KeyRangeRef &range) {
		transaction.clear(range);
	}

	//Processes transaction error conditions
	Future<Void> onError(Error const& e) {
		Future<Void> returnVal = transaction.onError(e);
		if( useExtraDB ) {
			lastTransaction = std::move(transaction);
			transaction = T( g_random->random01() < 0.5 ? extraDB : cx );
		}
		return returnVal;
	}

	//Gets the read version of a transaction
	Future<Version> getReadVersion() {
		return transaction.getReadVersion();
	}

	//Gets the committed version of a transaction
	Version getCommittedVersion() {
		return transaction.getCommittedVersion();
	}

	//Prints debugging messages for a transaction
	void debugTransaction(UID debugId) {
		transaction.debugTransaction(debugId);
	}

	void addReadConflictRange( KeyRangeRef const& keys ) {
		transaction.addReadConflictRange(keys);
	}
};

//A wrapper class for ThreadSafeTransactions.  Converts ThreadFutures into Futures for interchangeability with flow transactions
struct ThreadTransactionWrapper : public TransactionWrapper {

	Reference<ITransaction> transaction;

	ThreadTransactionWrapper(Reference<IDatabase> db, Reference<IDatabase> extraDB, bool useExtraDB) : transaction(db->createTransaction()) { }
	virtual ~ThreadTransactionWrapper() { }

	//Sets a key-value pair in the database
	void set(KeyRef &key, ValueRef &value) {
		transaction->set(key, value);
	}

	//Commits modifications to the database
	Future<Void> commit() {
		return unsafeThreadFutureToFuture(transaction->commit());
	}

	//Gets a value associated with a given key from the database
	Future<Optional<Value>> get(KeyRef &key) {
		return unsafeThreadFutureToFuture(transaction->get(key));
	}

	//Gets a range of key-value pairs from the database specified by a key range
	Future<Standalone<RangeResultRef>> getRange(KeyRangeRef &keys, int limit, bool reverse) {
		return unsafeThreadFutureToFuture(transaction->getRange(keys, limit, false, reverse));
	}

	//Gets a range of key-value pairs from the database specified by a pair of key selectors
	Future<Standalone<RangeResultRef>> getRange(KeySelectorRef &begin, KeySelectorRef &end, int limit, bool reverse) {
		return unsafeThreadFutureToFuture(transaction->getRange(begin, end, limit, false, reverse));
	}

	//Gets the key from the database specified by a given key selector
	Future<Key> getKey(KeySelectorRef &key) {
		return unsafeThreadFutureToFuture(transaction->getKey(key));
	}

	//Clears a key from the database
	void clear(KeyRef &key) {
		transaction->clear(key);
	}

	//Clears a range of keys from the database
	void clear(KeyRangeRef &range) {
		transaction->clear(range);
	}

	//Processes transaction error conditions
	Future<Void> onError(Error const& e) {
		return unsafeThreadFutureToFuture(transaction->onError(e));
	}

	//Gets the read version of a transaction
	Future<Version> getReadVersion() {
		return unsafeThreadFutureToFuture(transaction->getReadVersion());
	}

	//Gets the committed version of a transaction
	Version getCommittedVersion() {
		return transaction->getCommittedVersion();
	}

	void addReadConflictRange( KeyRangeRef const& keys ) {
		transaction->addReadConflictRange(keys);
	}
};

//A factory interface for creating different kinds of TransactionWrappers
struct TransactionFactoryInterface : public ReferenceCounted<TransactionFactoryInterface> {
	virtual ~TransactionFactoryInterface() { }

	//Creates a new transaction
	virtual Reference<TransactionWrapper> createTransaction() = 0;
};

//Templated implementation of TransactionFactoryInterface which creates a specific type of TransactionWrapper
template<class T, class DB>
struct TransactionFactory : public TransactionFactoryInterface {

	//The database used to create transaction (of type Database, Reference<ThreadSafeDatabase>, etc.)
	DB dbHandle;
	DB extraDbHandle;
	bool useExtraDB;

	TransactionFactory(DB dbHandle, DB extraDbHandle, bool useExtraDB) : dbHandle(dbHandle), extraDbHandle(extraDbHandle), useExtraDB(useExtraDB) { }
	virtual ~TransactionFactory() { }

	//Creates a new transaction
	Reference<TransactionWrapper> createTransaction() {
		return Reference<TransactionWrapper>(new T(dbHandle, extraDbHandle, useExtraDB));
	}
};

struct ApiWorkload : TestWorkload {
	bool useExtraDB;
	Database extraDB;

	ApiWorkload(WorkloadContext const& wcx, int maxClients = -1) : TestWorkload(wcx), success(true), transactionFactory(NULL), maxClients(maxClients) {
		clientPrefixInt = getOption(options, LiteralStringRef("clientId"), clientId);
		clientPrefix = format("%010d", clientPrefixInt);

		numKeys = getOption(options, LiteralStringRef("numKeys"), 5000);
		onlyLowerCase = getOption(options, LiteralStringRef("onlyLowerCase"), false);
		shortKeysRatio = getOption(options, LiteralStringRef("shortKeysRatio"), 0.5);
		minShortKeyLength = getOption(options, LiteralStringRef("minShortKeyLength"), 1);
		maxShortKeyLength = getOption(options, LiteralStringRef("maxShortKeyLength"), 3);
		minLongKeyLength = getOption(options, LiteralStringRef("minLongKeyLength"), 1);
		maxLongKeyLength = getOption(options, LiteralStringRef("maxLongKeyLength"), 128);
		minValueLength = getOption(options, LiteralStringRef("minValueLength"), 1);
		maxValueLength = getOption(options, LiteralStringRef("maxValueLength"), 10000);

		useExtraDB = g_simulator.extraDB != NULL;
		if(useExtraDB) {
			Reference<ClusterConnectionFile> extraFile(new ClusterConnectionFile(*g_simulator.extraDB));
			Reference<Cluster> extraCluster = Cluster::createCluster(extraFile, -1);
			extraDB = extraCluster->createDatabase(LiteralStringRef("DB")).get();
		}
	}

	Future<Void> setup(Database const& cx);
	Future<Void> start(Database const& cx);
	Future<bool> check(Database const& cx);

	//Compares the contents of this client's key-space in the database with the in-memory key-value store
	Future<bool> compareDatabaseToMemory();

	//Verifies that the results of a getRange are the same in the database and in memory
	bool compareResults(VectorRef<KeyValueRef> dbResults, VectorRef<KeyValueRef> storeResults, Version readVersion);

	//Generates a set of random key-value pairs with an optional prefix
	Standalone<VectorRef<KeyValueRef>> generateData(int numKeys, int minKeyLength, int maxKeyLength, int minValueLength, int maxValueLength, std::string prefix = "", bool allowDuplicates = true);

	//Generates a random key
	Key generateKey(VectorRef<KeyValueRef> const& data, int minKeyLength, int maxKeyLength, std::string prefix = "");

	//Generates a random key selector with a specified maximum offset
	KeySelector generateKeySelector(VectorRef<KeyValueRef> const& data, int maxOffset);

	//Selects a random key.  There is a <probabilityKeyExists> probability that the key will be chosen from the keyset in data, otherwise the key will
	//be a randomly generated key
	Key selectRandomKey(VectorRef<KeyValueRef> const& data, double probabilityKeyExists);

	//Generates a random value
	Value generateValue(int minValueLength, int maxValueLength);

	//Generates a random value
	Value generateValue();

	//Convenience function for reporting a test failure to trace log and stdout
	void testFailure(std::string reason);

	//Creates a random transaction factory to produce transaction of one of the TransactionType choices
	Future<Void> chooseTransactionFactory(Database const& cx, std::vector<TransactionType> const& choices);

	//Creates a new transaction using the current transaction factory
	Reference<TransactionWrapper> createTransaction();

	//Implemented by subclasses; called during the setup function to prepare the database
	virtual Future<Void> performSetup(Database const& cx) = 0;

	//Implemented by subclasses; called during the start function to run the tests
	virtual Future<Void> performTest(Database const& cx, Standalone<VectorRef<KeyValueRef>> const& data) = 0;

	//Returns whether or not success is false
	bool hasFailed();

	//Clears the keyspace used by this test
	Future<Void> clearKeyspace();

	//The maximum number of tester clients that will run the test
	int maxClients;

	//A key prefix used by this client.  This is so each client can operate on a key space without worrying about
	//the operations of other clients.  Otherwise, it would be challenging to maintain an in-memory representation
	//of what the database should contain
	std::string clientPrefix;
	int clientPrefixInt;

	//Whether or not the test passed
	bool success;

	//How many keys each client should generate to put in the database.  This may not be exact, as some keys may be
	//duplicates of each other
	int numKeys;

	//The ratio of keys which should have small length (to encourage collisions)
	double shortKeysRatio;

	//The minimum length of a short key
	int minShortKeyLength;

	//The maximum length of a short key
	int maxShortKeyLength;

	//The minimum length of a long key
	int minLongKeyLength;

	//The maximum length of a long key
	int maxLongKeyLength;

	//The minimum length of a value
	int minValueLength;

	//The maximum length of a value
	int maxValueLength;

	//If true, then random keys will only contain lower case letters.  Otherwise, they will contain all character values
	bool onlyLowerCase;

	//The in-memory representation of this client's key space
	MemoryKeyValueStore store;

	//The transaction factory used to create transactions in this run
	Reference<TransactionFactoryInterface> transactionFactory;
};

#endif
