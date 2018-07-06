/*
 * BlobStore.h
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

#include <map>
#include <functional>
#include "flow/flow.h"
#include "flow/Net2Packet.h"
#include "fdbclient/Knobs.h"
#include "IRateControl.h"
#include "HTTP.h"
#include "JSONDoc.h"

// Representation of all the things you need to connect to a blob store instance with some credentials.
// Reference counted because a very large number of them could be needed.
class BlobStoreEndpoint : public ReferenceCounted<BlobStoreEndpoint> {
public:
	struct Stats {
		Stats() : requests_successful(0), requests_failed(0), bytes_sent(0) {}
		Stats operator-(const Stats &rhs);
		void clear() { memset(this, sizeof(*this), 0); }
		json_spirit::mObject getJSON();

		int64_t requests_successful;
		int64_t requests_failed;
		int64_t bytes_sent;
	};

	static Stats s_stats;

	struct BlobKnobs {
		BlobKnobs();
		int connect_tries,
			connect_timeout,
			max_connection_life,
			request_tries,
			request_timeout,
			requests_per_second,
			multipart_max_part_size,
			multipart_min_part_size,
			concurrent_requests,
			concurrent_uploads,
			concurrent_lists,
			concurrent_reads_per_file,
			concurrent_writes_per_file,
			read_block_size,
			read_ahead_blocks,
			read_cache_blocks_per_file,
			max_send_bytes_per_second,
			max_recv_bytes_per_second;
		bool set(StringRef name, int value);
		std::string getURLParameters() const;
		static std::vector<std::string> getKnobDescriptions() {
			return {
				"connect_tries (or ct)                 Number of times to try to connect for each request.",
				"connect_timeout (or cto)              Number of seconds to wait for a connect request to succeed.",
				"max_connection_life (or mcl)          Maximum number of seconds to use a single TCP connection.",
				"request_tries (or rt)                 Number of times to try each request until a parseable HTTP response other than 429 is received.",
				"request_timeout (or rto)              Number of seconds to wait for a request to succeed after a connection is established.",
				"requests_per_second (or rps)          Max number of requests to start per second.",
				"multipart_max_part_size (or maxps)    Max part size for multipart uploads.",
				"multipart_min_part_size (or minps)    Min part size for multipart uploads.",
				"concurrent_requests (or cr)           Max number of total requests in progress at once, regardless of operation-specific concurrency limits.",
				"concurrent_uploads (or cu)            Max concurrent uploads (part or whole) that can be in progress at once.",
				"concurrent_lists (or cl)              Max concurrent list operations that can be in progress at once.",
				"concurrent_reads_per_file (or crps)   Max concurrent reads in progress for any one file.",
				"concurrent_writes_per_file (or cwps)  Max concurrent uploads in progress for any one file.",
				"read_block_size (or rbs)              Block size in bytes to be used for reads.",
				"read_ahead_blocks (or rab)            Number of blocks to read ahead of requested offset.",
				"read_cache_blocks_per_file (or rcb)   Size of the read cache for a file in blocks.",
				"max_send_bytes_per_second (or sbps)   Max send bytes per second for all requests combined.",
				"max_recv_bytes_per_second (or rbps)   Max receive bytes per second for all requests combined (NOT YET USED)."
			};
		}
	};

	BlobStoreEndpoint(std::string const &host, std::string service, std::string const &key, std::string const &secret, BlobKnobs const &knobs = BlobKnobs())
	  : host(host), service(service), key(key), secret(secret), lookupSecret(secret.empty()), knobs(knobs),
		requestRate(new SpeedLimit(knobs.requests_per_second, 1)),
		sendRate(new SpeedLimit(knobs.max_send_bytes_per_second, 1)),
		recvRate(new SpeedLimit(knobs.max_recv_bytes_per_second, 1)),
		concurrentRequests(knobs.concurrent_requests),
		concurrentUploads(knobs.concurrent_uploads),
		concurrentLists(knobs.concurrent_lists) {

		if(host.empty())
			throw connection_string_invalid();
	}

	static std::string getURLFormat(bool withResource = false) {
		const char *resource = "";
		if(withResource)
			resource = "<name>";
		return format("blobstore://<api_key>:<secret>@<host>[:<port>]/%s[?<param>=<value>[&<param>=<value>]...]", resource);
	}
	static Reference<BlobStoreEndpoint> fromString(std::string const &url, std::string *resourceFromURL = nullptr, std::string *error = nullptr);

	// Get a normalized version of this URL with the given resource and any non-default BlobKnob values as URL parameters.
	std::string getResourceURL(std::string resource);

	struct ReusableConnection {
		Reference<IConnection> conn;
		double expirationTime;
	};
	std::queue<ReusableConnection> connectionPool;
	Future<ReusableConnection> connect();
	void returnConnection(ReusableConnection &conn);

	std::string host;
	std::string service;
	std::string key;
	std::string secret;
	bool lookupSecret;
	BlobKnobs knobs;

	// Speed and concurrency limits
	Reference<IRateControl> requestRate;
	Reference<IRateControl> sendRate;
	Reference<IRateControl> recvRate;
	FlowLock concurrentRequests;
	FlowLock concurrentUploads;
	FlowLock concurrentLists;

	Future<Void> updateSecret();

	// Calculates the authentication string from the secret key
	std::string hmac_sha1(std::string const &msg);

	// Sets headers needed for Authorization (including Date which will be overwritten if present)
	void setAuthHeaders(std::string const &verb, std::string const &resource, HTTP::Headers &headers);

	// Prepend the HTTP request header to the given PacketBuffer, returning the new head of the buffer chain
	static PacketBuffer * writeRequestHeader(std::string const &request, HTTP::Headers const &headers, PacketBuffer *dest);

	// Do an HTTP request to the Blob Store, read the response.  Handles authentication.
	// Every blob store interaction should ultimately go through this function

	Future<Reference<HTTP::Response>> doRequest(std::string const &verb, std::string const &resource, const HTTP::Headers &headers, UnsentPacketQueue *pContent, int contentLen, std::set<unsigned int> successCodes);

	struct ObjectInfo {
		std::string name;
		int64_t size;
	};

	struct ListResult {
		std::vector<std::string> commonPrefixes;
		std::vector<ObjectInfo> objects;
	};

	// Get bucket contents via a stream, since listing large buckets will take many serial blob requests
	// If a delimiter is passed then common prefixes will be read in parallel, recursively, depending on recurseFilter.
	// Recursefilter is a must be a function that takes a string and returns true if it passes.  The default behavior is to assume true.
	Future<Void> listBucketStream(std::string const &bucket, PromiseStream<ListResult> results, Optional<std::string> prefix = {}, Optional<char> delimiter = {}, int maxDepth = 0, std::function<bool(std::string const &)> recurseFilter = nullptr);

	// Get a list of the files in a bucket, see listBucketStream for more argument detail.
	Future<ListResult> listBucket(std::string const &bucket, Optional<std::string> prefix = {}, Optional<char> delimiter = {}, int maxDepth = 0, std::function<bool(std::string const &)> recurseFilter = nullptr);

	// Check if an object exists in a bucket
	Future<bool> objectExists(std::string const &bucket, std::string const &object);

	// Get the size of an object in a bucket
	Future<int64_t> objectSize(std::string const &bucket, std::string const &object);

	// Read an arbitrary segment of an object
	Future<int> readObject(std::string const &bucket, std::string const &object, void *data, int length, int64_t offset);

	// Delete an object in a bucket
	Future<Void> deleteObject(std::string const &bucket, std::string const &object);

	// Delete all objects in a bucket under a prefix.  Note this is not atomic as blob store does not
	// support this operation directly. This method is just a convenience method that lists and deletes
	// all of the objects in the bucket under the given prefix.
	// Since it can take a while, if a pNumDeleted is provided then it will be incremented every time
	// a deletion of an object completes.
	Future<Void> deleteRecursively(std::string const &bucket, std::string prefix = "", int *pNumDeleted = NULL);

	// Create a bucket if it does not already exists.
	Future<Void> createBucket(std::string const &bucket);

	// Useful methods for working with tiny files
	Future<std::string> readEntireFile(std::string const &bucket, std::string const &object);
	Future<Void>       writeEntireFile(std::string const &bucket, std::string const &object, std::string const &content);
	Future<Void>       writeEntireFileFromBuffer(std::string const &bucket, std::string const &object, UnsentPacketQueue *pContent, int contentLen, std::string const &contentMD5);

	// MultiPart upload methods
	// Returns UploadID
	Future<std::string> beginMultiPartUpload(std::string const &bucket, std::string const &object);
	// Returns eTag
	Future<std::string> uploadPart(std::string const &bucket, std::string const &object, std::string const &uploadID, unsigned int partNumber, UnsentPacketQueue *pContent, int contentLen, std::string const &contentMD5);
	typedef std::map<int, std::string> MultiPartSetT;
	Future<Void> finishMultiPartUpload(std::string const &bucket, std::string const &object, std::string const &uploadID, MultiPartSetT const &parts);
};

