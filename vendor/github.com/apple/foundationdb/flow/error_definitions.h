/*
 * error_definitions.h
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

#ifdef ERROR

// SOMEDAY: Split this into flow, fdbclient, fdbserver error headers?

// Error codes defined here are primarily for programmatic use, not debugging: a separate
// error should be defined if and only if there is a sensible situation in which code could
// catch and react specifically to that error.  So for example there is only one
// internal_error code even though there are a huge number of internal errors; extra
// information is logged in the trace file.

// 1xxx Normal failure (plausibly these should not even be "errors", but they are failures of
//   the way operations are currently defined)
ERROR( success, 0, "Success" )
ERROR( end_of_stream, 1, "End of stream" )
ERROR( operation_failed, 1000, "Operation failed")
ERROR( wrong_shard_server, 1001, "Shard is not available from this server")
ERROR( timed_out, 1004, "Operation timed out" )
ERROR( coordinated_state_conflict, 1005, "Conflict occurred while changing coordination information" )
ERROR( all_alternatives_failed, 1006, "All alternatives failed" )
ERROR( transaction_too_old, 1007, "Transaction is too old to perform reads or be committed" )
ERROR( no_more_servers, 1008, "Not enough physical servers available" )
ERROR( future_version, 1009, "Request for future version" )
ERROR( movekeys_conflict, 1010, "Conflicting attempts to change data distribution" )
ERROR( tlog_stopped, 1011, "TLog stopped" )
ERROR( server_request_queue_full, 1012, "Server request queue is full" )
ERROR( not_committed, 1020, "Transaction not committed due to conflict with another transaction" )
ERROR( commit_unknown_result, 1021, "Transaction may or may not have committed" )
ERROR( transaction_cancelled, 1025, "Operation aborted because the transaction was cancelled" )
ERROR( connection_failed, 1026, "Network connection failed" )
ERROR( coordinators_changed, 1027, "Coordination servers have changed" )
ERROR( new_coordinators_timed_out, 1028, "New coordination servers did not respond in a timely way" )
ERROR( watch_cancelled, 1029, "Watch cancelled because storage server watch limit exceeded" )
ERROR( request_maybe_delivered, 1030, "Request may or may not have been delivered" )
ERROR( transaction_timed_out, 1031, "Operation aborted because the transaction timed out" )
ERROR( too_many_watches, 1032, "Too many watches currently set" )
ERROR( locality_information_unavailable, 1033, "Locality information not available" )
ERROR( watches_disabled, 1034, "Watches cannot be set if read your writes is disabled" )
ERROR( default_error_or, 1035, "Default error for an ErrorOr object" )
ERROR( accessed_unreadable, 1036, "Read or wrote an unreadable key" )
ERROR( process_behind, 1037, "Storage process does not have recent mutations" )
ERROR( database_locked, 1038, "Database is locked" )
ERROR( cluster_version_changed, 1039, "The protocol version of the cluster has changed" )
ERROR( external_client_already_loaded, 1040, "External client has already been loaded" )
ERROR( lookup_failed, 1041, "DNS lookup failed" )

ERROR( broken_promise, 1100, "Broken promise" )
ERROR( operation_cancelled, 1101, "Asynchronous operation cancelled" )
ERROR( future_released, 1102, "Future has been released" )
ERROR( connection_leaked, 1103, "Connection object leaked" )

ERROR( recruitment_failed, 1200, "Recruitment of a server failed" )   // Be careful, catching this will delete the data of a storage server or tlog permanently
ERROR( move_to_removed_server, 1201, "Attempt to move keys to a storage server that was removed" )
ERROR( worker_removed, 1202, "Normal worker shut down" )   // Be careful, catching this will delete the data of a storage server or tlog permanently
ERROR( master_recovery_failed, 1203, "Master recovery failed")
ERROR( master_max_versions_in_flight, 1204, "Master hit maximum number of versions in flight" )
ERROR( master_tlog_failed, 1205, "Master terminating because a TLog failed" )   // similar to tlog_stopped, but the tlog has actually died
ERROR( worker_recovery_failed, 1206, "Recovery of a worker process failed" )
ERROR( please_reboot, 1207, "Reboot of server process requested" )
ERROR( please_reboot_delete, 1208, "Reboot of server process requested, with deletion of state" )
ERROR( master_proxy_failed, 1209, "Master terminating because a Proxy failed" )
ERROR( master_resolver_failed, 1210, "Master terminating because a Resolver failed" )

// 15xx Platform errors
ERROR( platform_error, 1500, "Platform error" )
ERROR( large_alloc_failed, 1501, "Large block allocation failed" )
ERROR( performance_counter_error, 1502, "QueryPerformanceCounter error" )

ERROR( io_error, 1510, "Disk i/o operation failed" )
ERROR( file_not_found, 1511, "File not found" )
ERROR( bind_failed, 1512, "Unable to bind to network" )
ERROR( file_not_readable, 1513, "File could not be read" )
ERROR( file_not_writable, 1514, "File could not be written" )
ERROR( no_cluster_file_found, 1515, "No cluster file found in current directory or default location" )
ERROR( file_too_large, 1516, "File too large to be read" )
ERROR( non_sequential_op, 1517, "Non sequential file operation not allowed" )
ERROR( http_bad_response, 1518, "HTTP response was badly formed" )
ERROR( http_not_accepted, 1519, "HTTP request not accepted" )
ERROR( checksum_failed, 1520, "A data checksum failed" )
ERROR( io_timeout, 1521, "A disk IO operation failed to complete in a timely manner" )
ERROR( file_corrupt, 1522, "A structurally corrupt data file was detected" )
ERROR( http_request_failed, 1523, "HTTP response code indicated failure" )
ERROR( http_auth_failed, 1524, "HTTP request failed due to bad credentials" )


// 2xxx Attempt (presumably by a _client_) to do something illegal.  If an error is known to
// be internally caused, it should be 41xx
ERROR( client_invalid_operation, 2000, "Invalid API call" )
ERROR( commit_read_incomplete, 2002, "Commit with incomplete read" )
ERROR( test_specification_invalid, 2003, "Invalid test specification" )
ERROR( key_outside_legal_range, 2004, "Key outside legal range" )
ERROR( inverted_range, 2005, "Range begin key larger than end key" )
ERROR( invalid_option_value, 2006, "Option set with an invalid value" )
ERROR( invalid_option, 2007, "Option not valid in this context" )
ERROR( network_not_setup, 2008, "Action not possible before the network is configured" )
ERROR( network_already_setup, 2009, "Network can be configured only once" )
ERROR( read_version_already_set, 2010, "Transaction already has a read version set" )
ERROR( version_invalid, 2011, "Version not valid" )
ERROR( range_limits_invalid, 2012, "Range limits not valid" )
ERROR( invalid_database_name, 2013, "Database name must be 'DB'" )
ERROR( attribute_not_found, 2014, "Attribute not found in string" )
ERROR( future_not_set, 2015, "Future not ready" )
ERROR( future_not_error, 2016, "Future not an error" )
ERROR( used_during_commit, 2017, "Operation issued while a commit was outstanding" )
ERROR( invalid_mutation_type, 2018, "Unrecognized atomic mutation type" )
ERROR( attribute_too_large, 2019, "Attribute too large for type int" )
ERROR( transaction_invalid_version, 2020, "Transaction does not have a valid commit version" )
ERROR( no_commit_version, 2021, "Transaction is read-only and therefore does not have a commit version" )
ERROR( environment_variable_network_option_failed, 2022, "Environment variable network option could not be set" )
ERROR( transaction_read_only, 2023, "Attempted to commit a transaction specified as read-only" )

ERROR( incompatible_protocol_version, 2100, "Incompatible protocol version" )
ERROR( transaction_too_large, 2101, "Transaction exceeds byte limit" )
ERROR( key_too_large, 2102, "Key length exceeds limit" )
ERROR( value_too_large, 2103, "Value length exceeds limit" )
ERROR( connection_string_invalid, 2104, "Connection string invalid" )
ERROR( address_in_use, 2105, "Local address in use" )
ERROR( invalid_local_address, 2106, "Invalid local address" )
ERROR( tls_error, 2107, "TLS error" )
ERROR( unsupported_operation, 2108, "Operation is not supported" )

// 2200 - errors from bindings and official APIs
ERROR( api_version_unset, 2200, "API version is not set" )
ERROR( api_version_already_set, 2201, "API version may be set only once" )
ERROR( api_version_invalid, 2202, "API version not valid" )
ERROR( api_version_not_supported, 2203, "API version not supported" )
ERROR( exact_mode_without_limits, 2210, "EXACT streaming mode requires limits, but none were given" )

ERROR( invalid_tuple_data_type, 2250, "Unrecognized data type in packed tuple")
ERROR( invalid_tuple_index, 2251, "Tuple does not have element at specified index")
ERROR( key_not_in_subspace, 2252, "Cannot unpack key that is not in subspace" )
ERROR( manual_prefixes_not_enabled, 2253, "Cannot specify a prefix unless manual prefixes are enabled" )
ERROR( prefix_in_partition, 2254, "Cannot specify a prefix in a partition" )
ERROR( cannot_open_root_directory, 2255, "Root directory cannot be opened" )
ERROR( directory_already_exists, 2256, "Directory already exists" )
ERROR( directory_does_not_exist, 2257, "Directory does not exist" )
ERROR( parent_directory_does_not_exist, 2258, "Directory's parent does not exist" )
ERROR( mismatched_layer, 2259, "Directory has already been created with a different layer string" )
ERROR( invalid_directory_layer_metadata, 2260, "Invalid directory layer metadata" )
ERROR( cannot_move_directory_between_partitions, 2261, "Directory cannot be moved between partitions" )
ERROR( cannot_use_partition_as_subspace, 2262, "Directory partition cannot be used as subspace" )
ERROR( incompatible_directory_version, 2263, "Directory layer was created with an incompatible version" )
ERROR( directory_prefix_not_empty, 2264, "Database has keys stored at the prefix chosen by the automatic prefix allocator" )
ERROR( directory_prefix_in_use, 2265, "Directory layer already has a conflicting prefix" )
ERROR( invalid_destination_directory, 2266, "Target directory is invalid" )
ERROR( cannot_modify_root_directory, 2267, "Root directory cannot be modified" )
ERROR( invalid_uuid_size, 2268, "UUID is not sixteen bytes");

// 2300 - backup and restore errors
ERROR( backup_error, 2300, "Backup error")
ERROR( restore_error, 2301, "Restore error")
ERROR( backup_duplicate, 2311, "Backup duplicate request")
ERROR( backup_unneeded, 2312, "Backup unneeded request")
ERROR( backup_bad_block_size, 2313, "Backup file block size too small")
ERROR( backup_invalid_url, 2314, "Backup Container URL invalid")
ERROR( backup_invalid_info, 2315, "Backup Container URL invalid")
ERROR( backup_cannot_expire, 2316, "Cannot expire requested data from backup without violating minimum restorability")
ERROR( backup_auth_missing, 2317, "Cannot find authentication details (such as a password or secret key) for the specified Backup Container URL")
ERROR( backup_auth_unreadable, 2318, "Cannot read or parse one or more sources of authentication information for Backup Container URLs")
ERROR( restore_invalid_version, 2361, "Invalid restore version")
ERROR( restore_corrupted_data, 2362, "Corrupted backup data")
ERROR( restore_missing_data, 2363, "Missing backup data")
ERROR( restore_duplicate_tag, 2364, "Restore duplicate request")
ERROR( restore_unknown_tag, 2365, "Restore tag does not exist")
ERROR( restore_unknown_file_type, 2366, "Unknown backup/restore file type")
ERROR( restore_unsupported_file_version, 2367, "Unsupported backup file version")
ERROR( restore_bad_read, 2368, "Unexpected number of bytes read")
ERROR( restore_corrupted_data_padding, 2369, "Backup file has unexpected padding bytes")
ERROR( restore_destination_not_empty, 2370, "Attempted to restore into a non-empty destination database")
ERROR( restore_duplicate_uid, 2371, "Attempted to restore using a UID that had been used for an aborted restore")
ERROR( task_invalid_version, 2381, "Invalid task version")
ERROR( task_interrupted, 2382, "Task execution stopped due to timeout, abort, or completion by another worker")

ERROR( key_not_found, 2400, "Expected key is missing")

// 4xxx Internal errors (those that should be generated only by bugs) are decimal 4xxx
ERROR( unknown_error, 4000, "An unknown error occurred" )  // C++ exception not of type Error
ERROR( internal_error, 4100, "An internal error occurred" )

#undef ERROR
#endif
