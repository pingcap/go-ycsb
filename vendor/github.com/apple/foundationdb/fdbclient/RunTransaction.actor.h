/*
 * RunTransaction.actor.h
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

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source version.
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_RUNTRANSACTION_ACTOR_G_H)
	#define FDBCLIENT_RUNTRANSACTION_ACTOR_G_H
	#include "RunTransaction.actor.g.h"
#elif !defined(FDBCLIENT_RUNTRANSACTION_ACTOR_H)
	#define FDBCLIENT_RUNTRANSACTION_ACTOR_H

#include "flow/flow.h"
#include "ReadYourWrites.h"

ACTOR template < class Function >
Future<decltype(fake<Function>()(Reference<ReadYourWritesTransaction>()).getValue())>
runRYWTransaction(Database cx, Function func) {
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	loop{
		try {
			state decltype( fake<Function>()( Reference<ReadYourWritesTransaction>() ).getValue()) result = wait(func(tr));
			Void _ = wait(tr->commit());
			return result;
		}
		catch (Error& e) {
			Void _ = wait(tr->onError(e));
		}
	}
}

ACTOR template < class Function >
Future<decltype(fake<Function>()(Reference<ReadYourWritesTransaction>()).getValue())>
runRYWTransactionFailIfLocked(Database cx, Function func) {
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	loop{
		try {
			state decltype( fake<Function>()( Reference<ReadYourWritesTransaction>() ).getValue()) result = wait(func(tr));
			Void _ = wait(tr->commit());
			return result;
		}
		catch (Error& e) {
			if(e.code() == error_code_database_locked)
				throw;
			Void _ = wait(tr->onError(e));
		}
	}
}

ACTOR template < class Function >
Future<decltype(fake<Function>()(Reference<ReadYourWritesTransaction>()).getValue())>
runRYWTransactionNoRetry(Database cx, Function func) {
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	state decltype(fake<Function>()(Reference<ReadYourWritesTransaction>()).getValue()) result = wait(func(tr));
	Void _ = wait(tr->commit());
	return result;
}
#endif