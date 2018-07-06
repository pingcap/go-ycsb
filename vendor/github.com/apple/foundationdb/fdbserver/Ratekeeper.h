/*
 * Ratekeeper.h
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

#ifndef FDBSERVER_RATEKEEPER_H
#define FDBSERVER_RATEKEEPER_H
#pragma once

#include "MasterInterface.h"
#include "TLogInterface.h"
#include "DatabaseConfiguration.h"

Future<Void> rateKeeper(
	Reference<AsyncVar<struct ServerDBInfo>> const& dbInfo,
	PromiseStream< std::pair<UID, Optional<StorageServerInterface>> > const& serverChanges,  // actually an input, but we don't want broken_promise
	FutureStream< struct GetRateInfoRequest > const& getRateInfo,
	Standalone<StringRef> const& dbName,
	DatabaseConfiguration const& configuration,
	double* const& lastLimited);

#endif