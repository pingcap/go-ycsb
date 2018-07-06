/*
 * Net2FileSystem.h
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

#ifndef FLOW_NET2FILESYSTEM_H
#define FLOW_NET2FILESYSTEM_H
#pragma once

#include "IAsyncFile.h"

class Net2FileSystem : public IAsyncFileSystem {
public:
	virtual Future< Reference<class IAsyncFile> > open( std::string filename, int64_t flags, int64_t mode );
	// Opens a file for asynchronous I/O

	virtual Future< Void > deleteFile( std::string filename, bool mustBeDurable );
	// Deletes the given file.  If mustBeDurable, returns only when the file is guaranteed to be deleted even after a power failure.

	//void init();

	Net2FileSystem(double ioTimeout=0.0, std::string fileSystemPath = "");

	virtual ~Net2FileSystem() {}

	static void newFileSystem(double ioTimeout=0.0, std::string fileSystemPath = "");

#ifdef __linux__
	dev_t fileSystemDeviceId;
	bool checkFileSystem;
#endif
};

#endif
