// +build ldt
//
// Copyright 2013-2017 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aerospike

//-------------------------------------------------------------------
// Large collection functions (Supported by Aerospike 3 servers only)
//-------------------------------------------------------------------

// GetLargeList initializes large list operator.
// This operator can be used to create and manage a list
// within a single bin.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, the default relevant policy will be used.
// NOTICE: DEPRECATED ON SERVER. Will be removed in future. Use CDT operations instead.
func (clnt *Client) GetLargeList(policy *WritePolicy, key *Key, binName string, userModule string) *LargeList {
	policy = clnt.getUsableWritePolicy(policy)
	return NewLargeList(clnt, policy, key, binName, userModule)
}

// GetLargeMap initializes a large map operator.
// This operator can be used to create and manage a map
// within a single bin.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, the default relevant policy will be used.
// NOTICE: DEPRECATED ON SERVER. Will be removed in future. Use CDT operations instead.
func (clnt *Client) GetLargeMap(policy *WritePolicy, key *Key, binName string, userModule string) *LargeMap {
	policy = clnt.getUsableWritePolicy(policy)
	return NewLargeMap(clnt, policy, key, binName, userModule)
}

// GetLargeSet initializes large set operator.
// This operator can be used to create and manage a set
// within a single bin.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, the default relevant policy will be used.
// NOTICE: DEPRECATED ON SERVER. Will be removed in future.
func (clnt *Client) GetLargeSet(policy *WritePolicy, key *Key, binName string, userModule string) *LargeSet {
	policy = clnt.getUsableWritePolicy(policy)
	return NewLargeSet(clnt, policy, key, binName, userModule)
}

// GetLargeStack initializes large stack operator.
// This operator can be used to create and manage a stack
// within a single bin.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, the default relevant policy will be used.
// NOTICE: DEPRECATED ON SERVER. Will be removed in future.
func (clnt *Client) GetLargeStack(policy *WritePolicy, key *Key, binName string, userModule string) *LargeStack {
	policy = clnt.getUsableWritePolicy(policy)
	return NewLargeStack(clnt, policy, key, binName, userModule)
}
