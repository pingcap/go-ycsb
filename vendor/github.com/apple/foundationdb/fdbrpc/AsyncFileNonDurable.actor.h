/*
 * AsyncFileNonDurable.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(FLOW_ASYNCFILENONDURABLE_ACTOR_G_H)
	#define FLOW_ASYNCFILENONDURABLE_ACTOR_G_H
	#include "AsyncFileNonDurable.actor.g.h"
#elif !defined(FLOW_ASYNCFILENONDURABLE_ACTOR_H)
	#define FLOW_ASYNCFILENONDURABLE_ACTOR_H

#include "flow/flow.h"
#include "IAsyncFile.h"
#include "flow/ActorCollection.h"
#include "simulator.h"
#include "TraceFileIO.h"
#include "RangeMap.h"

#undef max
#undef min

Future<Void> sendOnProcess( ISimulator::ProcessInfo* const& process, Promise<Void> const& promise, int const& taskID );
Future<Void> sendErrorOnProcess( ISimulator::ProcessInfo* const& process, Promise<Void> const& promise, Error const& e, int const& taskID );

ACTOR template <class T> 
Future<T> sendErrorOnShutdown( Future<T> in ) {
	choose {
		when( ISimulator::KillType _ = wait( g_simulator.getCurrentProcess()->shutdownSignal.getFuture() ) ) {
			throw io_error().asInjectedFault();
		}
		when( T rep = wait( in ) ) {
			return rep;
		}
	}
}

class AsyncFileDetachable sealed : public IAsyncFile, public ReferenceCounted<AsyncFileDetachable>{
private:
	Reference<IAsyncFile> file;
	Future<Void> shutdown;

public:
	explicit AsyncFileDetachable( Reference<IAsyncFile> file ) : file(file) {
		shutdown = doShutdown(this);
	}

	ACTOR Future<Void> doShutdown( AsyncFileDetachable* self ) {
		ISimulator::KillType _ = wait( g_simulator.getCurrentProcess()->shutdownSignal.getFuture() );
		self->file = Reference<IAsyncFile>();
		return Void();
	}
	
	ACTOR static Future<Reference<IAsyncFile>> open( Future<Reference<IAsyncFile>> wrappedFile ) {
		choose {
			when( ISimulator::KillType _ = wait( g_simulator.getCurrentProcess()->shutdownSignal.getFuture() ) ) {
				throw io_error().asInjectedFault();
			}
			when( Reference<IAsyncFile> f = wait( wrappedFile ) ) {
				return Reference<AsyncFileDetachable>( new AsyncFileDetachable(f) );
			}
		}
	}

	virtual void addref() {
		ReferenceCounted<AsyncFileDetachable>::addref();
	}
	virtual void delref() {
		ReferenceCounted<AsyncFileDetachable>::delref();
	}

	Future<int> read(void *data, int length, int64_t offset) {
		if( !file.getPtr() || g_simulator.getCurrentProcess()->shutdownSignal.getFuture().isReady() )
			return io_error().asInjectedFault();
		return sendErrorOnShutdown( file->read( data, length, offset ) );
	}

	Future<Void> write(void const *data, int length, int64_t offset) {
		if( !file.getPtr() || g_simulator.getCurrentProcess()->shutdownSignal.getFuture().isReady() )
			return io_error().asInjectedFault();
		return sendErrorOnShutdown( file->write( data, length, offset ) );
	}
	
	Future<Void> truncate(int64_t size) {
		if( !file.getPtr() || g_simulator.getCurrentProcess()->shutdownSignal.getFuture().isReady() )
			return io_error().asInjectedFault();
		return sendErrorOnShutdown( file->truncate( size ) );
	}

	Future<Void> sync() {
		if( !file.getPtr() || g_simulator.getCurrentProcess()->shutdownSignal.getFuture().isReady() )
			return io_error().asInjectedFault();
		return sendErrorOnShutdown( file->sync() );
	}

	Future<int64_t> size() {
		if( !file.getPtr() || g_simulator.getCurrentProcess()->shutdownSignal.getFuture().isReady() )
			return io_error().asInjectedFault();
		return sendErrorOnShutdown( file->size() );
	}

	int64_t debugFD() {
		if( !file.getPtr() )
			throw io_error().asInjectedFault();
		return file->debugFD();
	}
	std::string getFilename() {
		if( !file.getPtr() )
			throw io_error().asInjectedFault();
		return file->getFilename();
	}
};

//An async file implementation which wraps another async file and will randomly destroy sectors that it is writing when killed
//This is used to simulate a power failure which prevents all written data from being persisted to disk
class AsyncFileNonDurable sealed : public IAsyncFile, public ReferenceCounted<AsyncFileNonDurable>{
public:
	UID id;
	std::string filename;

	//An approximation of the size of the file; .size() should be used instead of this variable in most cases
	int64_t approximateSize;

	//The address of the machine that opened the file
	NetworkAddress openedAddress;

private:
	//The wrapped IAsyncFile
	Reference<IAsyncFile> file;

	//The maximum amount of time a write is delayed before being passed along to the underlying file
	double maxWriteDelay;

	//Modifications which haven't been pushed to file, mapped by the location in the file that is being modified
	RangeMap< uint64_t, Future<Void> > pendingModifications;

	//Will be blocked whenever kill is running
	Promise<Void> killed;
	Promise<Void> killComplete;

	//Used by sync (and kill) to force writes which have not yet been passed along.
	//If true is sent, then writes will be durable.  If false, then they may not be durable.
	Promise<bool> startSyncPromise;

	//The performance parameters of the simulated disk
	Reference<DiskParameters> diskParameters;

	//Set to true the first time sync is called on the file
	bool hasBeenSynced;

	//Used to describe what corruption is allowed by the file as well as the type of corruption being used on a particular page
	enum KillMode { NO_CORRUPTION = 0, DROP_ONLY = 1, FULL_CORRUPTION = 2 };

	//Limits what types of corruption are applied to writes from this file
	KillMode killMode;

	ActorCollection reponses; //cannot call getResult on this actor collection, since the actors will be on different processes

	AsyncFileNonDurable(const std::string& filename, Reference<IAsyncFile> file, Reference<DiskParameters> diskParameters, NetworkAddress openedAddress) 
		: openedAddress(openedAddress), pendingModifications(uint64_t(-1)), approximateSize(0), reponses(false) {

		//This is only designed to work in simulation
		ASSERT(g_network->isSimulated());
		this->id = g_random->randomUniqueID();

		//TraceEvent("AsyncFileNonDurable_Create", id).detail("Filename", filename);
		this->file = file;
		this->filename = filename;
		this->diskParameters = diskParameters;
		maxWriteDelay = 5.0;
		hasBeenSynced = false;

		killMode = (KillMode)g_random->randomInt(1, 3);
		//TraceEvent("AsyncFileNonDurable_CreateEnd", id).detail("Filename", filename).backtrace();
	}

public:
	static std::map<std::string, Future<Void>> filesBeingDeleted;

	//Creates a new AsyncFileNonDurable which wraps the provided IAsyncFile
	ACTOR static Future<Reference<IAsyncFile>> open(std::string filename, std::string actualFilename, Future<Reference<IAsyncFile>> wrappedFile, Reference<DiskParameters> diskParameters) {
		state ISimulator::ProcessInfo* currentProcess = g_simulator.getCurrentProcess();
		state int currentTaskID = g_network->getCurrentTask();
		state Future<Void> shutdown = success(currentProcess->shutdownSignal.getFuture());

		//TraceEvent("AsyncFileNonDurableOpenBegin").detail("Filename", filename).detail("addr", g_simulator.getCurrentProcess()->address);
		Void _ = wait( g_simulator.onMachine( currentProcess ) );
		try {
			Void _ = wait(success(wrappedFile) || shutdown);

			if(shutdown.isReady())
				throw io_error().asInjectedFault();

			state Reference<IAsyncFile> file = wrappedFile.get();

			//If we are in the process of deleting a file, we can't let someone else modify it at the same time.  We therefore block the creation of new files until deletion is complete
			state std::map<std::string, Future<Void>>::iterator deletedFile = filesBeingDeleted.find(filename);
			if(deletedFile != filesBeingDeleted.end()) {
				//TraceEvent("AsyncFileNonDurableOpenWaitOnDelete1").detail("Filename", filename);
				Void _ = wait( deletedFile->second || shutdown );
				//TraceEvent("AsyncFileNonDurableOpenWaitOnDelete2").detail("Filename", filename);
				if(shutdown.isReady())
					throw io_error().asInjectedFault();
			}

			state Reference<AsyncFileNonDurable> nonDurableFile( new AsyncFileNonDurable(filename, file, diskParameters, currentProcess->address) );

			//Causes the approximateSize member to be set
			state Future<int64_t> sizeFuture = nonDurableFile->size();
			Void _ = wait(success(sizeFuture) || shutdown);

			if(shutdown.isReady())
				throw io_error().asInjectedFault();

			//TraceEvent("AsyncFileNonDurableOpenComplete").detail("Filename", filename);

			Void _ = wait( g_simulator.onProcess( currentProcess, currentTaskID ) );

			return nonDurableFile;
		} catch( Error &e ) {
			state Error err = e;
			std::string currentFilename = ( wrappedFile.isReady() && !wrappedFile.isError() ) ? wrappedFile.get()->getFilename() : actualFilename;
			currentProcess->machine->openFiles.erase( currentFilename );
			//TraceEvent("AsyncFileNonDurableOpenError").detail("Filename", filename).detail("address", currentProcess->address).error(e, true).detail("addr", g_simulator.getCurrentProcess()->address);
			Void _ = wait( g_simulator.onProcess( currentProcess, currentTaskID ) );
			throw err;
		}
	}

	~AsyncFileNonDurable() {
		//TraceEvent("AsyncFileNonDurable_Destroy", id).detail("Filename", filename);
	}

	virtual void addref() {
		ReferenceCounted<AsyncFileNonDurable>::addref();
	}
	virtual void delref() { 
		if(delref_no_destroy()) {
			ASSERT(filesBeingDeleted.count(filename) == 0);
			//TraceEvent("AsyncFileNonDurable_StartDelete", id).detail("Filename", filename);
			Future<Void> deleteFuture = deleteFile(this);
			if(!deleteFuture.isReady())
				filesBeingDeleted[filename] = deleteFuture;
		}
	}

	//Passes along reads straight to the underlying file, waiting for any outstanding changes that could affect the results
	Future<int> read(void *data, int length, int64_t offset) {
		return read(this, data, length, offset);
	}

	//Writes data to the file.  Writes are delayed a random amount of time before being
	//passed to the underlying file
	Future<Void> write(void const *data, int length, int64_t offset) {
		//TraceEvent("AsyncFileNonDurable_Write", id).detail("Filename", filename).detail("Offset", offset).detail("Length", length);
		if(length == 0) {
			TraceEvent(SevWarnAlways, "AsyncFileNonDurable_EmptyModification", id).detail("Filename", filename);
			return Void();
		}

		debugFileSet("AsyncFileNonDurableWrite", filename, data, offset, length);

		Promise<Void> writeStarted;
		Promise<Future<Void>> writeEnded;
		writeEnded.send(write(this, writeStarted, writeEnded.getFuture(), data, length, offset));
		return writeStarted.getFuture();
	}
	
	//Truncates the file.  Truncates are delayed a random amount of time before being
	//passed to the underlying file
	Future<Void> truncate(int64_t size) {
		//TraceEvent("AsyncFileNonDurable_Truncate", id).detail("Filename", filename).detail("Offset", size);
		debugFileTruncate("AsyncFileNonDurableTruncate", filename, size);

		Promise<Void> truncateStarted;
		Promise<Future<Void>> truncateEnded;
		truncateEnded.send(truncate(this, truncateStarted, truncateEnded.getFuture(), size));
		return truncateStarted.getFuture();
	}

	//Fsyncs the file.  This allows all delayed modifications to the file to complete before
	//syncing the underlying file
	Future<Void> sync() {
		//TraceEvent("AsyncFileNonDurable_Sync", id).detail("Filename", filename);
		Future<Void> syncFuture = sync(this, true);
		reponses.add( syncFuture );
		return syncFuture;
	}

	//Passes along size requests to the underlying file, augmenting with any writes past the end of the file
	Future<int64_t> size() {
		return size(this);
	}

	int64_t debugFD() {
		return file->debugFD();
	}

	std::string getFilename() {
		return file->getFilename();
	}

	//Forces a non-durable sync (some writes are not made or made incorrectly)
	//This is used when the file should 'die' without first completing its operations
	//(e.g. to simulate power failure)
	Future<Void> kill() {
		TraceEvent("AsyncFileNonDurable_Kill", id).detail("Filename", filename);
		TEST(true); //AsyncFileNonDurable was killed
		return sync(this, false);
	}

private:

	//Returns a future that is used to ensure the waiter ends up on the main thread
	Future<Void> returnToMainThread() {
		Promise<Void> p;
		Future<Void> f = p.getFuture();
		g_network->onMainThread(std::move(p), g_network->getCurrentTask());
		return f;
	}

	//Gets existing modifications that overlap the specified range.  Optionally inserts a new modification into the map
	std::vector<Future<Void>> getModificationsAndInsert(int64_t offset, int64_t length, bool insertModification = false, Future<Void> value = Void()) {
		auto modification = RangeMapRange<uint64_t>(offset, length>=0 ? offset+length : uint64_t(-1));
		auto priorModifications = pendingModifications.intersectingRanges(modification);

		//Aggregate existing modifications in this range
		std::vector<Future<Void>> modificationFutures;
		for(auto itr = priorModifications.begin(); itr != priorModifications.end(); ++itr) {
			if(itr.value().isValid() && (!itr.value().isReady() || itr.value().isError())) { 
				modificationFutures.push_back(itr.value());
			}
		}

		//Add the modification if we are doing a write or truncate
		if(insertModification)
			pendingModifications.insert(modification, value);

		return modificationFutures;
	}

	//Checks if the file is killed.  If so, then the current sync is completed if running and then an error is thrown
	ACTOR Future<Void> checkKilled(AsyncFileNonDurable *self, std::string context) {
		if(self->killed.isSet()) {
			//TraceEvent("AsyncFileNonDurable_KilledInCheck", self->id).detail("In", context).detail("Filename", self->filename);
			Void _ = wait(self->killComplete.getFuture());
			TraceEvent("AsyncFileNonDurable_KilledFileOperation", self->id).detail("In", context).detail("Filename", self->filename);
			TEST(true); // AsyncFileNonDurable operation killed
			throw io_error().asInjectedFault();
		}

		return Void();
	}

	//Passes along reads straight to the underlying file, waiting for any outstanding changes that could affect the results
	ACTOR Future<int> onRead(AsyncFileNonDurable *self, void *data, int length, int64_t offset) {
		Void _ = wait(self->checkKilled(self, "Read"));
		vector<Future<Void>> priorModifications = self->getModificationsAndInsert(offset, length);
		Void _ = wait(waitForAll(priorModifications));
		state Future<int> readFuture = self->file->read(data, length, offset);
		Void _ = wait( success( readFuture ) || self->killed.getFuture() );

		// throws if we were killed
		Void _ = wait(self->checkKilled(self, "ReadEnd"));

		debugFileCheck("AsyncFileNonDurableRead", self->filename, data, offset, length);

		//if(g_simulator.getCurrentProcess()->rebooting)
			//TraceEvent("AsyncFileNonDurable_ReadEnd", self->id).detail("Filename", self->filename);

		return readFuture.get();
	}

	ACTOR Future<int> read(AsyncFileNonDurable *self, void *data, int length, int64_t offset) {
		state ISimulator::ProcessInfo* currentProcess = g_simulator.getCurrentProcess();
		state int currentTaskID = g_network->getCurrentTask();
		Void _ = wait( g_simulator.onMachine( currentProcess ) );

		try {
			state int rep = wait( self->onRead( self, data, length, offset ) );
			Void _ = wait( g_simulator.onProcess( currentProcess, currentTaskID ) );

			return rep;
		} catch( Error &e ) {
			state Error err = e;
			Void _ = wait( g_simulator.onProcess( currentProcess, currentTaskID ) );
			throw err;
		}
	}

	//Delays writes a random amount of time before passing them through to the underlying file.
	//If a kill interrupts the delay, then the output could be the correct write, part of the write,
	//or none of the write.  It may also corrupt parts of sectors which have not been written correctly
	ACTOR Future<Void> write(AsyncFileNonDurable *self, Promise<Void> writeStarted, Future<Future<Void>> ownFuture, void const* data, int length, int64_t offset) {
		state ISimulator::ProcessInfo* currentProcess = g_simulator.getCurrentProcess();
		state int currentTaskID = g_network->getCurrentTask();
		Void _ = wait( g_simulator.onMachine( currentProcess ) );
		
		state double delayDuration = g_random->random01() * self->maxWriteDelay;
		state Standalone<StringRef> dataCopy(StringRef((uint8_t*)data, length));

		state Future<bool> startSyncFuture = self->startSyncPromise.getFuture();

		try {
			//TraceEvent("AsyncFileNonDurable_Write", self->id).detail("Delay", delayDuration).detail("Filename", self->filename).detail("WriteLength", length).detail("Offset", offset);
			Void _ = wait(self->checkKilled(self, "Write"));

			Future<Void> writeEnded = wait(ownFuture);
			std::vector<Future<Void>> priorModifications = self->getModificationsAndInsert(offset, length, true, writeEnded);

			if(BUGGIFY_WITH_PROB(0.001))
				priorModifications.push_back(delay(g_random->random01() * FLOW_KNOBS->MAX_PRIOR_MODIFICATION_DELAY) || self->killed.getFuture());
			else
				priorModifications.push_back(waitUntilDiskReady(self->diskParameters, length) || self->killed.getFuture());

			Void _ = wait(waitForAll(priorModifications));

			self->approximateSize = std::max(self->approximateSize, length + offset);

			self->reponses.add( sendOnProcess( currentProcess, writeStarted, currentTaskID ) );
		}
		catch(Error &e) {
			self->reponses.add( sendErrorOnProcess( currentProcess, writeStarted, e, currentTaskID ) );
			throw;
		}

		//TraceEvent("AsyncFileNonDurable_WriteDoneWithPreviousMods", self->id).detail("Delay", delayDuration).detail("Filename", self->filename).detail("WriteLength", length).detail("Offset", offset);

		//Wait a random amount of time or until a sync/kill is issued
		state bool saveDurable = true;
		choose {
			when(Void _ = wait(delay(delayDuration))) { }
			when(bool durable = wait(startSyncFuture)) {
				saveDurable = durable;
			}
		}

		debugFileCheck("AsyncFileNonDurableWriteAfterWait", self->filename, dataCopy.begin(), offset, length);

		//Only page-aligned writes are supported
		ASSERT(offset % 4096 == 0 && length % 4096 == 0);

		//Non-durable writes should introduce errors at the page level and corrupt at the sector level
		//Otherwise, we can perform the entire write at once
		int pageLength = saveDurable ? length : 4096;
		int sectorLength = saveDurable ? length : 512;

		vector<Future<Void>> writeFutures;
		for(int writeOffset = 0; writeOffset < length; writeOffset += pageLength) {
			//choose a random action to perform on this page write (write correctly, corrupt, or don't write)
			KillMode pageKillMode = (KillMode)g_random->randomInt(0, self->killMode + 1);
		
			for(int pageOffset = 0; pageOffset < pageLength; pageOffset += sectorLength) {
				//If saving durable, then perform the write correctly.  Otherwise, perform the write correcly with a probability of 1/3.
				//If corrupting the write, then this sector will be written correctly with a 1/4 chance
				if(saveDurable || pageKillMode == NO_CORRUPTION || (pageKillMode == FULL_CORRUPTION && g_random->random01() < 0.25)) {
					//if (!saveDurable) TraceEvent(SevInfo, "AsyncFileNonDurableWrite", self->id).detail("Filename", self->filename).detail("Offset", offset+writeOffset+pageOffset).detail("Length", sectorLength);
					writeFutures.push_back(self->file->write(dataCopy.begin() + writeOffset + pageOffset, sectorLength, offset + writeOffset + pageOffset));
				}

				//If the write is not durable, then the write will either be corrupted or not written at all.  If corrupted, there is 1/4 chance that a given
				//sector will not be written
				else if(pageKillMode == FULL_CORRUPTION && g_random->random01() < 0.66667) {
					//The incorrect part of the write can be the rightmost bytes (side = 0), the leftmost bytes (side = 1), or the entire write (side = 2)
					int side = g_random->randomInt(0, 3);

					//There is a 1/2 chance that a bad write will have garbage written into its bad portion
					//The chance is increased to 1 if the entire write is bad
					bool garbage = side == 2 || g_random->random01() < 0.5;

					int64_t goodStart = 0;
					int64_t goodEnd = sectorLength;
					int64_t badStart = 0;
					int64_t badEnd = sectorLength;

					if(side == 0) {
						goodEnd = g_random->randomInt(0, sectorLength);
						badStart = goodEnd;
					}
					else if(side == 1) {
						badEnd = g_random->randomInt(0, sectorLength);
						goodStart = badEnd;
					}
					else
						goodEnd = 0;

					//Write randomly generated bytes, if required
					if(garbage && badStart != badEnd) {
						uint8_t *badData = const_cast<uint8_t*>(&dataCopy.begin()[badStart + writeOffset + pageOffset]);
						for(int i = 0; i < badEnd - badStart; i += sizeof(uint32_t)) {
							uint32_t val = g_random->randomUInt32();
							memcpy(&badData[i], &val, std::min(badEnd - badStart - i, (int64_t)sizeof(uint32_t)));
						}

						writeFutures.push_back(self->file->write(dataCopy.begin() + writeOffset + pageOffset, sectorLength, offset + writeOffset + pageOffset));
						debugFileSet("AsyncFileNonDurableBadWrite", self->filename, dataCopy.begin() + writeOffset + pageOffset, offset + writeOffset + pageOffset, sectorLength);
					}
					else if(goodStart != goodEnd)
						writeFutures.push_back(self->file->write(dataCopy.begin() + goodStart + writeOffset + pageOffset, goodEnd - goodStart, goodStart + offset + writeOffset + pageOffset));

					TraceEvent("AsyncFileNonDurable_BadWrite", self->id).detail("Offset", offset + writeOffset + pageOffset).detail("Length", sectorLength).detail("GoodStart", goodStart).detail("GoodEnd", goodEnd).detail("HasGarbage", garbage).detail("Side", side).detail("Filename", self->filename);
					TEST(true); //AsyncFileNonDurable bad write
				}
				else {
					TraceEvent("AsyncFileNonDurable_DroppedWrite", self->id).detail("Offset", offset + writeOffset + pageOffset).detail("Length", sectorLength).detail("Filename", self->filename);
					TEST(true); //AsyncFileNonDurable dropped write
				}
			}
		}
			
		Void _ = wait(waitForAll(writeFutures));
		//TraceEvent("AsyncFileNonDurable_WriteDone", self->id).detail("Delay", delayDuration).detail("Filename", self->filename).detail("WriteLength", length).detail("Offset", offset);
		return Void();
	}

	//Delays truncates a random amount of time before passing them through to the underlying file.
	//If a kill interrupts the delay, then the truncate may or may not be performed
	ACTOR Future<Void> truncate(AsyncFileNonDurable *self, Promise<Void> truncateStarted, Future<Future<Void>> ownFuture, int64_t size) {
		state ISimulator::ProcessInfo* currentProcess = g_simulator.getCurrentProcess();
		state int currentTaskID = g_network->getCurrentTask();
		Void _ = wait( g_simulator.onMachine( currentProcess ) );
		
		state double delayDuration = g_random->random01() * self->maxWriteDelay;
		state Future<bool> startSyncFuture = self->startSyncPromise.getFuture();

		try {
			//TraceEvent("AsyncFileNonDurable_Truncate", self->id).detail("Delay", delayDuration).detail("Filename", self->filename);
			Void _ = wait(self->checkKilled(self, "Truncate"));

			Future<Void> truncateEnded = wait(ownFuture);
			std::vector<Future<Void>> priorModifications = self->getModificationsAndInsert(size, -1, true, truncateEnded);

			if(BUGGIFY_WITH_PROB(0.001))
				priorModifications.push_back(delay(g_random->random01() * FLOW_KNOBS->MAX_PRIOR_MODIFICATION_DELAY) || self->killed.getFuture());
			else
				priorModifications.push_back(waitUntilDiskReady(self->diskParameters, 0) || self->killed.getFuture());

			Void _ = wait(waitForAll(priorModifications));

			self->approximateSize = size;

			self->reponses.add( sendOnProcess( currentProcess, truncateStarted, currentTaskID ) );
		}
		catch(Error &e) {
			self->reponses.add( sendErrorOnProcess( currentProcess, truncateStarted, e, currentTaskID ) );
			throw;
		}

		//Wait a random amount of time or until a sync/kill is issued
		state bool saveDurable = true;
		choose {
			when(Void _ = wait(delay(delayDuration))) { }
			when(bool durable = wait(startSyncFuture)) {
				saveDurable = durable;
			}
		}

		if(g_network->check_yield(TaskDefaultYield)) {
			Void _ = wait(delay(0, TaskDefaultYield));
		}

		//If performing a durable truncate, then pass it through to the file.  Otherwise, pass it through with a 1/2 chance
		if(saveDurable || self->killMode == NO_CORRUPTION || g_random->random01() < 0.5)
			Void _ = wait(self->file->truncate(size));
		else {
			TraceEvent("AsyncFileNonDurable_DroppedTruncate", self->id).detail("Size", size);
			TEST(true); //AsyncFileNonDurable dropped truncate
		}

		return Void();
	}

	//Waits for delayed modifications to the file to complete and then syncs the underlying file
	//If durable is false, then some of the delayed modifications will not be applied or will be
	//applied incorrectly
	ACTOR Future<Void> onSync(AsyncFileNonDurable *self, bool durable) {
		//TraceEvent("AsyncFileNonDurable_ImplSync", self->id).detail("Filename", self->filename).detail("Durable", durable);
		ASSERT(durable || !self->killed.isSet()); // this file is kill()ed only once

		if(durable) {
			self->hasBeenSynced = true;
			Void _ = wait(waitUntilDiskReady(self->diskParameters, 0, true) || self->killed.getFuture());
		}

		Void _ = wait(self->checkKilled(self, durable ? "Sync" : "Kill"));
		
		if(!durable)
			self->killed.send( Void() );

		//Get all outstanding modifications
		std::vector<Future<Void>> outstandingModifications;
		std::vector<RangeMapRange<uint64_t>> stillPendingModifications;

		auto rangeItr = self->pendingModifications.ranges();
		for(auto itr = rangeItr.begin(); itr != rangeItr.end(); ++itr) {
			if(itr.value().isValid() && (!itr->value().isReady() || itr->value().isError())) {
				outstandingModifications.push_back(itr->value());

				if(!itr.value().isReady())
					stillPendingModifications.push_back(itr->range());
			}
		}

		Future<Void> allModifications = waitForAll(outstandingModifications);
		//Clear out the pending modifications map of all completed modifications
		self->pendingModifications.insert(RangeMapRange<uint64_t>(0, -1), Void());
		for(auto itr = stillPendingModifications.begin(); itr != stillPendingModifications.end(); ++itr)
			self->pendingModifications.insert(*itr, success(allModifications)); //waitForAll cannot wait on the same future more than once, so wrap the future with success

		//Signal all modifications to end their delay and reset the startSyncPromise
		Promise<bool> startSyncPromise = self->startSyncPromise;
		self->startSyncPromise = Promise<bool>();

		//Writes will be durable in a kill with a 10% probability
		state bool writeDurable = durable || g_random->random01() < 0.1;
		startSyncPromise.send(writeDurable);

		//Wait for outstanding writes to complete
		if(durable)
			Void _ = wait(allModifications);
		else
			ErrorOr<Void> _ = wait(errorOr(allModifications));

		if(!durable) {
			//Sometimes sync the file if writes were made durably.  Before a file is first synced, it is stored in a temporary file and then renamed to the correct
			//location once sync is called.  By not calling sync, we simulate a failure to fsync the directory storing the file
			if(self->hasBeenSynced && writeDurable && g_random->random01() < 0.5) {
				TEST(true); //AsyncFileNonDurable kill was durable and synced
				ErrorOr<Void> _ = wait(errorOr(self->file->sync()));
			}

			//Setting this promise could trigger the deletion of the AsyncFileNonDurable; after this none of its members should be used
			//TraceEvent("AsyncFileNonDurable_ImplSyncEnd", self->id).detail("Filename", self->filename).detail("Durable", durable);
			self->killComplete.send(Void());
		}
		//A killed file cannot be allowed to report that it successfully synced
		else {
			Void _ = wait(self->checkKilled(self, "SyncEnd"));
			Void _ = wait(self->file->sync());
			//TraceEvent("AsyncFileNonDurable_ImplSyncEnd", self->id).detail("Filename", self->filename).detail("Durable", durable);
		}

		return Void();
	}

	ACTOR Future<Void> sync(AsyncFileNonDurable *self, bool durable) {
		state ISimulator::ProcessInfo* currentProcess = g_simulator.getCurrentProcess();
		state int currentTaskID = g_network->getCurrentTask();
		Void _ = wait( g_simulator.onMachine( currentProcess ) );

		try {
			Void _ = wait( self->onSync( self, durable ) );
			Void _ = wait( g_simulator.onProcess( currentProcess, currentTaskID ) );

			return Void();
		} catch( Error &e ) {
			state Error err = e;
			Void _ = wait( g_simulator.onProcess( currentProcess, currentTaskID ) );
			throw err;
		}
	}

	//Passes along size requests to the underlying file, augmenting with any writes past the end of the file
	ACTOR Future<int64_t> onSize(AsyncFileNonDurable *self) {
		//TraceEvent("AsyncFileNonDurable_Size", self->id).detail("Filename", self->filename);
		Void _ = wait(self->checkKilled(self, "Size"));
		state Future<int64_t> sizeFuture = self->file->size();
		Void _ = wait( success( sizeFuture ) || self->killed.getFuture() );

		Void _ = wait(self->checkKilled(self, "SizeEnd"));

		//Include any modifications which extend past the end of the file
		uint64_t maxModification = self->pendingModifications.lastItem().begin();
		self->approximateSize = std::max<int64_t>(sizeFuture.get(), maxModification);
		return self->approximateSize;
	}

	ACTOR Future<int64_t> size(AsyncFileNonDurable *self) {
		state ISimulator::ProcessInfo* currentProcess = g_simulator.getCurrentProcess();
		state int currentTaskID = g_network->getCurrentTask();

		Void _ = wait( g_simulator.onMachine( currentProcess ) );

		try {
			state int64_t rep = wait( self->onSize( self ) );
			Void _ = wait( g_simulator.onProcess( currentProcess, currentTaskID ) );

			return rep;
		} catch( Error &e ) {
			state Error err = e;
			Void _ = wait( g_simulator.onProcess( currentProcess, currentTaskID ) );
			throw err;
		}
	}

	//Finishes all outstanding actors on an AsyncFileNonDurable and then deletes it
	ACTOR Future<Void> deleteFile(AsyncFileNonDurable *self) {
		state ISimulator::ProcessInfo* currentProcess = g_simulator.getCurrentProcess();
		state int currentTaskID = g_network->getCurrentTask();
		state std::string filename = self->filename;

		Void _ = wait( g_simulator.onMachine( currentProcess ) );
		try {
			//Make sure all writes have gone through.
			Promise<bool> startSyncPromise = self->startSyncPromise;
			self->startSyncPromise = Promise<bool>();
			startSyncPromise.send(true);

			std::vector<Future<Void>> outstandingModifications;

			for(auto itr = self->pendingModifications.ranges().begin(); itr != self->pendingModifications.ranges().end(); ++itr)
				if(itr->value().isValid() && !itr->value().isReady())
					outstandingModifications.push_back(itr->value());

			//Ignore errors here so that all modifications can finish
			Void _ = wait(waitForAllReady(outstandingModifications));

			//Make sure we aren't in the process of killing the file
			if(self->killed.isSet())
				Void _ = wait(self->killComplete.getFuture());

			//Remove this file from the filesBeingDeleted map so that new files can be created with this filename
			g_simulator.getMachineByNetworkAddress( self->openedAddress )->closingFiles.erase(self->getFilename());
			g_simulator.getMachineByNetworkAddress( self->openedAddress )->deletingFiles.erase(self->getFilename());
			AsyncFileNonDurable::filesBeingDeleted.erase(self->filename);
			//TraceEvent("AsyncFileNonDurable_FinishDelete", self->id).detail("Filename", self->filename);

			delete self;
			Void _ = wait( g_simulator.onProcess( currentProcess, currentTaskID ) );
			return Void();
		} catch( Error &e ) {
			state Error err = e;
			Void _ = wait( g_simulator.onProcess( currentProcess, currentTaskID ) );
			throw err;
		}
	}
};

#endif
