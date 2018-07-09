/*
 * AsyncFileReadAhead.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(FDBRPC_ASYNCFILEREADAHEAD_ACTOR_G_H)
	#define FDBRPC_ASYNCFILEREADAHEAD_ACTOR_G_H
	#include "AsyncFileReadAhead.actor.g.h"
#elif !defined(FDBRPC_ASYNCFILEREADAHEAD_ACTOR_H)
	#define FDBRPC_ASYNCFILEREADAHEAD_ACTOR_H

#include "flow/flow.h"
#include "IAsyncFile.h"

// Read-only file type that wraps another file instance, reads in large blocks, and reads ahead of the actual range requested
class AsyncFileReadAheadCache : public IAsyncFile, public ReferenceCounted<AsyncFileReadAheadCache> {
public:
	virtual void addref() { ReferenceCounted<AsyncFileReadAheadCache>::addref(); }
	virtual void delref() { ReferenceCounted<AsyncFileReadAheadCache>::delref(); }

	struct CacheBlock : ReferenceCounted<CacheBlock> {
		CacheBlock(int size = 0) : data(new uint8_t[size]), len(size) {}
		~CacheBlock() { delete [] data; }
		uint8_t *data;
		int len;
	};

	// Read from the underlying file to a CacheBlock
	ACTOR static Future<Reference<CacheBlock>> readBlock(AsyncFileReadAheadCache *f, int length, int64_t offset) {
		Void _ = wait(f->m_max_concurrent_reads.take());

		state Reference<CacheBlock> block(new CacheBlock(length));
		try {
			int len = wait(f->m_f->read(block->data, length, offset));
			block->len = len;
		} catch(Error &e) {
			f->m_max_concurrent_reads.release(1);
			throw e;
		}

		f->m_max_concurrent_reads.release(1);
		return block;
	}

	ACTOR static Future<int> read_impl(Reference<AsyncFileReadAheadCache> f, void *data, int length, int64_t offset) {
		// Make sure range is valid for the file
		int64_t fileSize = wait(f->size());
		if(offset >= fileSize)
			return 0;  // TODO:  Should this throw since the input isn't really valid?

		// If reading past the end then clip length to just read to the end
		if(offset + length > fileSize)
			length = fileSize - offset;   // Length is at least 1 since offset < fileSize

		// Calculate block range for the blocks that contain this data
		state int firstBlockNum = offset / f->m_block_size;
		state int lastBlockNum = (offset + length) / f->m_block_size;
		state int blockNum;

		// Start reads (if needed) of the block range required for this read, plus the read ahead blocks
		// The futures for the read started will be stored in the cache but since things can be evicted from
		// the cache while we're wait()ing we also will keep a local cache of futures for the blocks
		// we need (not the read ahead blocks).
		state std::map<int, Future<Reference<CacheBlock>>> localCache;

		// Start blocks up to the read ahead size beyond the last needed block but don't go past the end of the file
		state int lastBlockNumInFile = ((fileSize + f->m_block_size - 1) / f->m_block_size) - 1;
		int lastBlockToStart = std::min<int>(lastBlockNum + f->m_read_ahead_blocks, lastBlockNumInFile);

		for(blockNum = firstBlockNum; blockNum <= lastBlockToStart; ++blockNum) {
			Future<Reference<CacheBlock>> fblock;

			// Look in the per-file cache for the block's future
			auto i = f->m_blocks.find(blockNum);
			// If not found, start the read.
			if(i == f->m_blocks.end() || (i->second.isValid() && i->second.isError())) {
				//printf("starting read of %s block %d\n", f->getFilename().c_str(), blockNum);
				fblock = readBlock(f.getPtr(), f->m_block_size, f->m_block_size * blockNum);
				f->m_blocks[blockNum] = fblock;
			}
			else
				fblock = i->second;

			// Only put blocks we actually need into our local cache
			if(blockNum <= lastBlockNum)
				localCache[blockNum] = fblock;
		}

		// Read block(s) and copy data
		state int wpos = 0;
		for(blockNum = firstBlockNum; blockNum <= lastBlockNum; ++blockNum) {
			// Wait for block to be ready
			Reference<CacheBlock> block = wait(localCache[blockNum]);

			// Calculate the block-relative read range.  It's a given that the offset / length range touches this block so readStart will never
			// be greater than blocksize (though it could be past the actual end of a short block).
			int64_t blockStart = blockNum * f->m_block_size;
			int64_t readStart = std::max<int64_t>(0, offset - blockStart);
			int64_t readEnd = std::min<int64_t>(f->m_block_size, offset + length - blockStart);
			int rlen = readEnd - readStart;
			memcpy((uint8_t *)data + wpos, block->data + readStart, rlen);
			wpos += rlen;
		}

		ASSERT(wpos == length);
		localCache.clear();

		// If the cache is too large then go through the cache in block number order and remove any entries whose future
		// has a reference count of 1, stopping once the cache is no longer too big.  There is no point in removing
		// an entry from the cache if it has a reference count of > 1 because it will continue to exist and use memory
		// anyway so it should be left in the cache so that other readers may benefit from it.

		//printf("cache block limit: %d   Cache contents:\n", f->m_cache_block_limit);
		//for(auto &m : f->m_blocks) printf("\tblock %d refcount %d\n", m.first, m.second.getFutureReferenceCount());

		if(f->m_blocks.size() > f->m_cache_block_limit) {
			auto i = f->m_blocks.begin();
			while(i != f->m_blocks.end()) {
				if(i->second.getFutureReferenceCount() == 1) {
					//printf("evicting block %d\n", i->first);
					i = f->m_blocks.erase(i);
					if(f->m_blocks.size() <= f->m_cache_block_limit)
						break;
				}
				else
					++i;
			}
		}

		return wpos;
	}

	virtual Future<int> read( void *data, int length, int64_t offset ) {
		return read_impl(Reference<AsyncFileReadAheadCache>::addRef(this), data, length, offset);
	}

	virtual Future<Void> write( void const *data, int length, int64_t offset ) { throw file_not_writable(); }
	virtual Future<Void> truncate( int64_t size ) { throw file_not_writable(); }

	virtual Future<Void> sync() { return Void(); }
	virtual Future<Void> flush() { return Void(); }

	virtual Future<int64_t> size() { return m_f->size(); }

	virtual Future<Void> readZeroCopy( void** data, int* length, int64_t offset ) {
		TraceEvent(SevError, "ReadZeroCopyNotSupported").detail("FileType", "ReadAheadCache");
		return platform_error();
	}
	virtual void releaseZeroCopy( void* data, int length, int64_t offset ) {}

	virtual int64_t debugFD() { return -1; }

	virtual std::string getFilename() { return m_f->getFilename(); }

	virtual ~AsyncFileReadAheadCache() {
		for(auto &it : m_blocks) {
			it.second.cancel();
		}
	}

	Reference<IAsyncFile> m_f;
	int m_block_size;
	int m_read_ahead_blocks;
	int m_cache_block_limit;
	FlowLock m_max_concurrent_reads;

	// Map block numbers to future
	std::map<int, Future<Reference<CacheBlock>>> m_blocks;

	AsyncFileReadAheadCache(Reference<IAsyncFile> f, int blockSize, int readAheadBlocks, int maxConcurrentReads, int cacheSizeBlocks)
		: m_f(f), m_block_size(blockSize), m_read_ahead_blocks(readAheadBlocks), m_max_concurrent_reads(maxConcurrentReads),
		  m_cache_block_limit(std::max<int>(1, cacheSizeBlocks)) {
	}

};

#endif