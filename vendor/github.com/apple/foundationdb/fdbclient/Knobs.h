/*
 * Knobs.h
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

#ifndef FDBCLIENT_KNOBS_H
#define FDBCLIENT_KNOBS_H
#pragma once

#include "flow/Knobs.h"
#include "flow/flow.h"

class ClientKnobs : public Knobs {
public:
	int BYTE_LIMIT_UNLIMITED;
	int ROW_LIMIT_UNLIMITED;

	int TOO_MANY; // FIXME: this should really be split up so we can control these more specifically

	double SYSTEM_MONITOR_INTERVAL;

	double FAILURE_MAX_DELAY;
	double FAILURE_MIN_DELAY;
	double FAILURE_TIMEOUT_DELAY;
	double CLIENT_FAILURE_TIMEOUT_DELAY;

	// wrong_shard_server sometimes comes from the only nonfailed server, so we need to avoid a fast spin
	double WRONG_SHARD_SERVER_DELAY; // SOMEDAY: This delay can limit performance of retrieving data when the cache is mostly wrong (e.g. dumping the database after a test)
	double FUTURE_VERSION_RETRY_DELAY;
	int REPLY_BYTE_LIMIT;
	double DEFAULT_BACKOFF;
	double DEFAULT_MAX_BACKOFF;
	double BACKOFF_GROWTH_RATE;

	int64_t TRANSACTION_SIZE_LIMIT;
	int64_t KEY_SIZE_LIMIT;
	int64_t SYSTEM_KEY_SIZE_LIMIT;
	int64_t VALUE_SIZE_LIMIT;
	int64_t SPLIT_KEY_SIZE_LIMIT;

	int MAX_BATCH_SIZE;
	double GRV_BATCH_TIMEOUT;

	// When locationCache in DatabaseContext gets to be this size, items will be evicted
	int LOCATION_CACHE_EVICTION_SIZE;
	int LOCATION_CACHE_EVICTION_SIZE_SIM;

	int GET_RANGE_SHARD_LIMIT;
	int WARM_RANGE_SHARD_LIMIT;
	int STORAGE_METRICS_SHARD_LIMIT;
	double STORAGE_METRICS_UNFAIR_SPLIT_LIMIT;
	double STORAGE_METRICS_TOO_MANY_SHARDS_DELAY;

	//KeyRangeMap
	int KRM_GET_RANGE_LIMIT;
	int KRM_GET_RANGE_LIMIT_BYTES; //This must be sufficiently larger than KEY_SIZE_LIMIT to ensure that at least two entries will be returned from an attempt to read a key range map

	int DEFAULT_MAX_OUTSTANDING_WATCHES;
	int ABSOLUTE_MAX_WATCHES; //The client cannot set the max outstanding watches higher than this
	double WATCH_POLLING_TIME;

	double IS_ACCEPTABLE_DELAY;


	// Core
	int64_t CORE_VERSIONSPERSECOND;  // This is defined within the server but used for knobs based on server value
	int LOG_RANGE_BLOCK_SIZE;
	int MUTATION_BLOCK_SIZE;

	// Taskbucket
	int TASKBUCKET_MAX_PRIORITY;
	double TASKBUCKET_CHECK_TIMEOUT_CHANCE;
	double TASKBUCKET_TIMEOUT_JITTER_OFFSET;
	double TASKBUCKET_TIMEOUT_JITTER_RANGE;
	double TASKBUCKET_CHECK_ACTIVE_DELAY;
	int TASKBUCKET_CHECK_ACTIVE_AMOUNT;
	int TASKBUCKET_TIMEOUT_VERSIONS;
	int TASKBUCKET_MAX_TASK_KEYS;

	// Backup
	int BACKUP_CONCURRENT_DELETES;
	int BACKUP_SIMULATED_LIMIT_BYTES;
	int BACKUP_GET_RANGE_LIMIT_BYTES;
	int BACKUP_LOCK_BYTES;
	double BACKUP_RANGE_TIMEOUT;
	double BACKUP_RANGE_MINWAIT;
	int BACKUP_SNAPSHOT_DISPATCH_INTERVAL_SEC;
	int BACKUP_DEFAULT_SNAPSHOT_INTERVAL_SEC;
	int BACKUP_SHARD_TASK_LIMIT;
	double BACKUP_AGGREGATE_POLL_RATE;
	double BACKUP_AGGREGATE_POLL_RATE_UPDATE_INTERVAL;
	int BACKUP_LOG_WRITE_BATCH_MAX_SIZE;
	int BACKUP_LOG_ATOMIC_OPS_SIZE;
	int BACKUP_MAX_LOG_RANGES;
	int BACKUP_SIM_COPY_LOG_RANGES;
	int BACKUP_OPERATION_COST_OVERHEAD;
	int BACKUP_VERSION_DELAY;
	int BACKUP_MAP_KEY_LOWER_LIMIT;
	int BACKUP_MAP_KEY_UPPER_LIMIT;
	int BACKUP_COPY_TASKS;
	int BACKUP_BLOCK_SIZE;
	int BACKUP_TASKS_PER_AGENT;
	int SIM_BACKUP_TASKS_PER_AGENT;
	int BACKUP_RANGEFILE_BLOCK_SIZE;
	int BACKUP_LOGFILE_BLOCK_SIZE;
	int BACKUP_DISPATCH_ADDTASK_SIZE;
	int RESTORE_DISPATCH_ADDTASK_SIZE;
	int RESTORE_DISPATCH_BATCH_SIZE;
	int RESTORE_WRITE_TX_SIZE;
	int APPLY_MAX_LOCK_BYTES;
	int APPLY_MIN_LOCK_BYTES;
	int APPLY_BLOCK_SIZE;
	double APPLY_MAX_DECAY_RATE;
	double APPLY_MAX_INCREASE_FACTOR;
	double BACKUP_ERROR_DELAY;
	double BACKUP_STATUS_DELAY;
	double BACKUP_STATUS_JITTER;

	// Configuration
	int32_t DEFAULT_AUTO_PROXIES;
	int32_t DEFAULT_AUTO_RESOLVERS;
	int32_t DEFAULT_AUTO_LOGS;

	// Client Status Info
	double CSI_SAMPLING_PROBABILITY;
	int64_t CSI_SIZE_LIMIT;
	double CSI_STATUS_DELAY;

	int HTTP_SEND_SIZE;
	int HTTP_READ_SIZE;
	int HTTP_VERBOSE_LEVEL;
	int BLOBSTORE_CONNECT_TRIES;
	int BLOBSTORE_CONNECT_TIMEOUT;
	int BLOBSTORE_MAX_CONNECTION_LIFE;
	int BLOBSTORE_REQUEST_TRIES;
	int BLOBSTORE_REQUEST_TIMEOUT;
	int BLOBSTORE_REQUESTS_PER_SECOND;
	int BLOBSTORE_CONCURRENT_REQUESTS;
	int BLOBSTORE_MULTIPART_MAX_PART_SIZE;
	int BLOBSTORE_MULTIPART_MIN_PART_SIZE;
	int BLOBSTORE_CONCURRENT_UPLOADS;
	int BLOBSTORE_CONCURRENT_LISTS;
	int BLOBSTORE_CONCURRENT_WRITES_PER_FILE;
	int BLOBSTORE_CONCURRENT_READS_PER_FILE;
	int BLOBSTORE_READ_BLOCK_SIZE;
	int BLOBSTORE_READ_AHEAD_BLOCKS;
	int BLOBSTORE_READ_CACHE_BLOCKS_PER_FILE;
	int BLOBSTORE_MAX_SEND_BYTES_PER_SECOND;
	int BLOBSTORE_MAX_RECV_BYTES_PER_SECOND;

	int CONSISTENCY_CHECK_RATE_LIMIT;
	int CONSISTENCY_CHECK_RATE_WINDOW;

	ClientKnobs(bool randomize = false);
};

extern ClientKnobs const* CLIENT_KNOBS;

#endif
