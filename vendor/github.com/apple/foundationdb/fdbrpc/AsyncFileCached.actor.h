/*
 * AsyncFileCached.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(FLOW_ASYNCFILECACHED_ACTOR_G_H)
	#define FLOW_ASYNCFILECACHED_ACTOR_G_H
	#include "AsyncFileCached.actor.g.h"
#elif !defined(FLOW_ASYNCFILECACHED_ACTOR_H)
	#define FLOW_ASYNCFILECACHED_ACTOR_H

#include "flow/flow.h"
#include "IAsyncFile.h"
#include "flow/Knobs.h"
#include "flow/TDMetric.actor.h"
#include "flow/network.h"

struct EvictablePage {
	void* data;
	int index;
	class Reference<struct EvictablePageCache> pageCache;

	virtual bool evict() = 0; // true if page was evicted, false if it isn't immediately evictable (but will be evicted regardless if possible)

	EvictablePage(Reference<EvictablePageCache> pageCache) : data(0), index(-1), pageCache(pageCache) {}
	virtual ~EvictablePage();
};

struct EvictablePageCache : ReferenceCounted<EvictablePageCache> {
	EvictablePageCache() : pageSize(0), maxPages(0) {}
	explicit EvictablePageCache(int pageSize, int64_t maxSize) : pageSize(pageSize), maxPages(maxSize / pageSize) {}

	void allocate(EvictablePage* page) {
		try_evict();
		try_evict();
		page->data = pageSize == 4096 ? FastAllocator<4096>::allocate() : aligned_alloc(4096,pageSize);
		page->index = pages.size();
		pages.push_back(page);
	}

	void try_evict() {
		if (pages.size() >= (uint64_t)maxPages && !pages.empty()) {
			for (int i = 0; i < FLOW_KNOBS->MAX_EVICT_ATTEMPTS; i++) { // If we don't manage to evict anything, just go ahead and exceed the cache limit
				int toEvict = g_random->randomInt(0, pages.size());
				if (pages[toEvict]->evict())
					break;
			}
		}
	}

	std::vector<EvictablePage*> pages;
	int pageSize;
	int64_t maxPages;
};

struct OpenFileInfo : NonCopyable {
	IAsyncFile* f;
	Future<Reference<IAsyncFile>> opened; // Only valid until the file is fully opened

	OpenFileInfo() : f(0) {}
	OpenFileInfo(OpenFileInfo && r) noexcept(true) : f(r.f), opened(std::move(r.opened)) { r.f = 0; }

	Future<Reference<IAsyncFile>> get() {
		if (f) return Reference<IAsyncFile>::addRef(f);
		else return opened;
	}
};

struct AFCPage;

class AsyncFileCached : public IAsyncFile, public ReferenceCounted<AsyncFileCached> {
	friend struct AFCPage;

public:
	static Future<Reference<IAsyncFile>> open( std::string filename, int flags, int mode ) {
		//TraceEvent("AsyncFileCachedOpen").detail("Filename", filename);
		if ( openFiles.find(filename) == openFiles.end() ) {
			auto f = open_impl( filename, flags, mode );
			if ( f.isReady() && f.isError() )
				return f;
			if( !f.isReady() )
				openFiles[filename].opened = f;
			else
				return f.get();
		}
		return openFiles[filename].get();
	}

	virtual Future<int> read( void* data, int length, int64_t offset ) {
		++countFileCacheReads;
		++countCacheReads;
		if (offset + length > this->length) {
			length = int(this->length - offset);
			ASSERT(length >= 0);
		}
		auto f = read_write_impl(this, data, length, offset, false);
		if( f.isReady() && !f.isError() ) return length;
		++countFileCacheReadsBlocked;
		++countCacheReadsBlocked;
		return tag(f,length);
	}

	virtual Future<Void> write( void const* data, int length, int64_t offset ) {
		++countFileCacheWrites;
		++countCacheWrites;
		auto f = read_write_impl(this, const_cast<void*>(data), length, offset, true);
		if (!f.isReady()) {
			++countFileCacheWritesBlocked;
			++countCacheWritesBlocked;
		}
		return f;
	}

	virtual Future<Void> readZeroCopy( void** data, int* length, int64_t offset );
	virtual void releaseZeroCopy( void* data, int length, int64_t offset );

	virtual Future<Void> truncate( int64_t size );

	ACTOR Future<Void> truncate_impl( AsyncFileCached* self, int64_t size, Future<Void> truncates ) {
		Void _ = wait( truncates );
		Void _ = wait( self->uncached->truncate( size ) );
		return Void();
	}

	virtual Future<Void> sync() {
		return waitAndSync( this, flush() );
	}

	virtual Future<int64_t> size() {
		return length;
	}

	virtual int64_t debugFD() {
		return uncached->debugFD();
	}

	virtual std::string getFilename() {
		return filename;
	}

	virtual void addref() { 
		ReferenceCounted<AsyncFileCached>::addref(); 
		//TraceEvent("AsyncFileCachedAddRef").detail("Filename", filename).detail("Refcount", debugGetReferenceCount()).backtrace();
	}
	virtual void delref() {
		if (delref_no_destroy()) {
			// If this is ever ThreadSafeReferenceCounted...
			// setrefCountUnsafe(0);

			auto f = quiesce();
			//TraceEvent("AsyncFileCachedDel").detail("Filename", filename)
			//	.detail("Refcount", debugGetReferenceCount()).detail("CanDie", f.isReady()).backtrace();
			if (f.isReady())
				delete this;
			else
				uncancellable( holdWhile( Reference<AsyncFileCached>::addRef( this ), f ) );
		}
	}

	~AsyncFileCached();

private:
	static std::map< std::string, OpenFileInfo > openFiles;
	std::string filename;
	Reference<IAsyncFile> uncached;
	int64_t length;
	int64_t prevLength;
	std::unordered_map<int64_t, AFCPage*> pages;
	std::vector<AFCPage*> flushable;
	Reference<EvictablePageCache> pageCache;

	Int64MetricHandle countFileCacheFinds;
	Int64MetricHandle countFileCacheReads;
	Int64MetricHandle countFileCacheWrites;
	Int64MetricHandle countFileCacheReadsBlocked;
	Int64MetricHandle countFileCacheWritesBlocked;
	Int64MetricHandle countFileCachePageReadsMerged;
	Int64MetricHandle countFileCacheReadBytes;

	Int64MetricHandle countCacheFinds;
	Int64MetricHandle countCacheReads;
	Int64MetricHandle countCacheWrites;
	Int64MetricHandle countCacheReadsBlocked;
	Int64MetricHandle countCacheWritesBlocked;
	Int64MetricHandle countCachePageReadsMerged;
	Int64MetricHandle countCacheReadBytes;

	AsyncFileCached( Reference<IAsyncFile> uncached, const std::string& filename, int64_t length, Reference<EvictablePageCache> pageCache ) 
		: uncached(uncached), filename(filename), length(length), prevLength(length), pageCache(pageCache) {
		if( !g_network->isSimulated() ) {
			countFileCacheWrites.init(         LiteralStringRef("AsyncFile.CountFileCacheWrites"), filename);
			countFileCacheReads.init(          LiteralStringRef("AsyncFile.CountFileCacheReads"), filename);
			countFileCacheWritesBlocked.init(  LiteralStringRef("AsyncFile.CountFileCacheWritesBlocked"), filename);
			countFileCacheReadsBlocked.init(   LiteralStringRef("AsyncFile.CountFileCacheReadsBlocked"), filename);
			countFileCachePageReadsMerged.init(LiteralStringRef("AsyncFile.CountFileCachePageReadsMerged"), filename);
			countFileCacheFinds.init(          LiteralStringRef("AsyncFile.CountFileCacheFinds"), filename);
			countFileCacheReadBytes.init(      LiteralStringRef("AsyncFile.CountFileCacheReadBytes"), filename);

			countCacheWrites.init(         LiteralStringRef("AsyncFile.CountCacheWrites"));
			countCacheReads.init(          LiteralStringRef("AsyncFile.CountCacheReads"));
			countCacheWritesBlocked.init(  LiteralStringRef("AsyncFile.CountCacheWritesBlocked"));
			countCacheReadsBlocked.init(   LiteralStringRef("AsyncFile.CountCacheReadsBlocked"));
			countCachePageReadsMerged.init(LiteralStringRef("AsyncFile.CountCachePageReadsMerged"));
			countCacheFinds.init(          LiteralStringRef("AsyncFile.CountCacheFinds"));
			countCacheReadBytes.init(      LiteralStringRef("AsyncFile.CountCacheReadBytes"));

		}
	}

	static Future<Reference<IAsyncFile>> open_impl( std::string filename, int flags, int mode );

	ACTOR static Future<Reference<IAsyncFile>> open_impl( std::string filename, int flags, int mode, Reference<EvictablePageCache> pageCache ) {
		try {
			TraceEvent("AFCUnderlyingOpenBegin").detail("filename", filename);
			if(flags & IAsyncFile::OPEN_CACHED_READ_ONLY)
				flags = flags & ~IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_READONLY;
			else
				flags = flags & ~IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_READWRITE;
			state Reference<IAsyncFile> f = wait( IAsyncFileSystem::filesystem()->open(filename, flags | IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_UNBUFFERED, mode) );
			TraceEvent("AFCUnderlyingOpenEnd").detail("filename", filename);
			int64_t l = wait( f->size() );
			TraceEvent("AFCUnderlyingSize").detail("filename", filename).detail("size", l);
			auto& of = openFiles[filename];
			of.f = new AsyncFileCached(f, filename, l, pageCache);
			of.opened = Future<Reference<IAsyncFile>>();
			return Reference<IAsyncFile>( of.f );
		} catch (Error& e) {
			if( e.code() != error_code_actor_cancelled )
				openFiles.erase( filename );
			throw e;
		}
	}

	virtual Future<Void> flush();

	Future<Void> quiesce();

	ACTOR static Future<Void> waitAndSync( AsyncFileCached* self, Future<Void> flush ) {
		Void _ = wait( flush );
		Void _ = wait( self->uncached->sync() );
		return Void();
	}

	static Future<Void> read_write_impl( AsyncFileCached* self, void* data, int length, int64_t offset, bool writing );

	static Future<Void> truncate_impl( AsyncFileCached* self, int64_t size );

	void remove_page( AFCPage* page );
};

struct AFCPage : public EvictablePage, public FastAllocated<AFCPage> {
	virtual bool evict() {
		if ( notReading.isReady() && notFlushing.isReady() && !dirty && !zeroCopyRefCount && !truncated ) {
			owner->remove_page( this );
			delete this;
			return true;
		}

		if (dirty)
			flush();

		return false;
	}

	Future<Void> write( void const* data, int length, int offset ) {
		ASSERT( !zeroCopyRefCount );  // overlapping zero-copy reads and writes are undefined behavior
		setDirty();

		if (valid || (offset == 0 && length == pageCache->pageSize)) {
			valid = true;
			memcpy( static_cast<uint8_t*>(this->data) + offset, data, length );
			return yield();
		}

		if (notReading.isReady()) {
			notReading = readThrough( this );
		}

		notReading = waitAndWrite( this, data, length, offset );

		return notReading;
	}

	ACTOR static Future<Void> waitAndWrite( AFCPage* self, void const* data, int length, int offset ) {
		Void _ = wait( self->notReading );
		memcpy( static_cast<uint8_t*>(self->data) + offset, data, length );
		return Void();
	}

	Future<Void> readZeroCopy() {
		++zeroCopyRefCount;
		if (valid) return yield();

		if (notReading.isReady()) {
			notReading = readThrough( this );
		} else {
			++owner->countFileCachePageReadsMerged;
			++owner->countCachePageReadsMerged;
		}

		return notReading;
	}
	void releaseZeroCopy() {
		--zeroCopyRefCount;
		ASSERT( zeroCopyRefCount >= 0 );
	}

	Future<Void> read( void* data, int length, int offset ) {
		if (valid) {
			owner->countFileCacheReadBytes += length;
			owner->countCacheReadBytes += length;
			memcpy( data, static_cast<uint8_t const*>(this->data) + offset, length );
			return yield();
		}

		if (notReading.isReady()) {
			notReading = readThrough( this );
		} else {
			++owner->countFileCachePageReadsMerged;
			++owner->countCachePageReadsMerged;
		}

		notReading = waitAndRead( this, data, length, offset );

		return notReading;
	}

	ACTOR static Future<Void> waitAndRead( AFCPage* self, void* data, int length, int offset ) {
		Void _ = wait( self->notReading );
		memcpy( data, static_cast<uint8_t const*>(self->data) + offset, length );
		return Void();
	}

	ACTOR static Future<Void> readThrough( AFCPage* self ) {
		ASSERT(!self->valid);
		if ( self->pageOffset < self->owner->prevLength ) {
			try {
				int _ = wait( self->owner->uncached->read( self->data, self->pageCache->pageSize, self->pageOffset ) );
				if (_ != self->pageCache->pageSize)
					TraceEvent("ReadThroughShortRead").detail("ReadAmount", _).detail("PageSize", self->pageCache->pageSize).detail("PageOffset", self->pageOffset);
			} catch (Error& e) {
				self->zeroCopyRefCount = 0;
				TraceEvent("ReadThroughFailed").error(e);
				throw;
			}
		}
		self->valid = true;
		return Void();
	}

	ACTOR static Future<Void> writeThrough( AFCPage* self, Promise<Void> writing ) {
		// writeThrough can be called on a page that is not dirty, just to wait for a previous writeThrough to finish.  In that
		// case we don't want to do any disk I/O
		try {
			state bool dirty = self->dirty;
			++self->writeThroughCount;
			self->updateFlushableIndex();

			Void _ = wait( self->notReading && self->notFlushing );

			if (dirty) {
				if ( self->pageOffset + self->pageCache->pageSize > self->owner->length ) {
					ASSERT(self->pageOffset < self->owner->length);
					memset( static_cast<uint8_t *>(self->data) + self->owner->length - self->pageOffset, 0, self->pageCache->pageSize - (self->owner->length - self->pageOffset) );
				}

				auto f = self->owner->uncached->write( self->data, self->pageCache->pageSize, self->pageOffset );

				Void _ = wait( f );
			}
		}
		catch(Error& e) {
			--self->writeThroughCount;
			self->setDirty();
			writing.sendError(e);
			throw;
		}
		--self->writeThroughCount;
		self->updateFlushableIndex();

		writing.send(Void()); // FIXME: This could happen before the wait if AsyncFileKAIO dealt properly with overlapping write and sync operations

		self->pageCache->try_evict();

		return Void();
	}

	Future<Void> flush() {
		if (!dirty && notFlushing.isReady()) return Void();

		ASSERT(valid || !notReading.isReady() || notReading.isError());

		Promise<Void> writing;

		notFlushing = writeThrough( this, writing );

		clearDirty(); // Do this last so that if writeThrough immediately calls try_evict, we can't be evicted before assigning notFlushing
		return writing.getFuture();
	}

	Future<Void> quiesce() {
		if (dirty) flush();

		// If we are flushing, we will be quiescent when all flushes are finished
		// Returning flush() isn't right, because flush can return before notFlushing.isReady()
		if (!notFlushing.isReady()) {
			return notFlushing;
		}

		// else if we are reading, we will be quiescent when the read is finished
		if ( !notReading.isReady() )
			return notReading;

		return Void();
	}

	Future<Void> truncate() {
		ASSERT( !zeroCopyRefCount );  // overlapping zero-copy reads and writes are undefined behavior
		truncated = true;
		return truncate_impl( this );
	}

	ACTOR static Future<Void> truncate_impl( AFCPage* self ) {
		Void _ = wait( self->notReading && self->notFlushing && yield() );
		delete self;
		return Void();
	}

	AFCPage( AsyncFileCached* owner, int64_t offset ) : EvictablePage(owner->pageCache), owner(owner), pageOffset(offset), dirty(false), valid(false), truncated(false), notReading(Void()), notFlushing(Void()), zeroCopyRefCount(0), flushableIndex(-1), writeThroughCount(0) {
		pageCache->allocate(this);
	}

	virtual ~AFCPage() {
		clearDirty();
		ASSERT( flushableIndex == -1 );
	}

	void setDirty() {
		dirty = true;
		updateFlushableIndex();
	}

	void clearDirty() {
		dirty = false;
		updateFlushableIndex();
	}

	void updateFlushableIndex() {
		bool flushable = dirty || writeThroughCount;
		if (flushable == (flushableIndex != -1)) return;

		if (flushable) {
			flushableIndex = owner->flushable.size();
			owner->flushable.push_back(this);
		} else {
			ASSERT( owner->flushable[flushableIndex] == this );
			owner->flushable[flushableIndex] = owner->flushable.back();
			owner->flushable[flushableIndex]->flushableIndex = flushableIndex;
			owner->flushable.pop_back();
			flushableIndex = -1;
		}
	}

	AsyncFileCached* owner;
	int64_t pageOffset;

	Future<Void> notReading; // .isReady when a readThrough (or waitAndWrite) is not in progress
	Future<Void> notFlushing; // .isReady when a writeThrough is not in progress

	bool dirty; // write has been called more recently than flush
	bool valid; // data contains the file contents
	bool truncated; // true if this page has been truncated
	int writeThroughCount;  // number of writeThrough actors that are in progress (potentially writing or waiting to write)
	int flushableIndex;  // index in owner->flushable[]
	int zeroCopyRefCount;  // references held by "zero-copy" reads
};

#endif
