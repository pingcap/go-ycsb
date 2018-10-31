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

package types

import (
	"errors"
	"strings"
)

// AerospikeError implements error interface for aerospike specific errors.
// All errors returning from the library are of this type.
// Errors resulting from Go's stdlib are not translated to this type, unless
// they are a net.Timeout error.
type AerospikeError struct {
	error

	resultCode ResultCode
	inDoubt    bool
}

// ResultCode returns the ResultCode from AerospikeError object.
func (ase AerospikeError) ResultCode() ResultCode {
	return ase.resultCode
}

// InDoubt determines if a write transaction may have completed or not.
func (ase AerospikeError) InDoubt() bool {
	return ase.inDoubt
}

// SetInDoubt sets whether it is possible that the write transaction may have completed
// even though this error was generated.  This may be the case when a
// client error occurs (like timeout) after the command was sent to the server.
func (ase *AerospikeError) SetInDoubt(isRead bool, commandSentCounter int) {
	if !isRead && (commandSentCounter > 1 || (commandSentCounter == 1 && (ase.resultCode == TIMEOUT || ase.resultCode <= 0))) {
		ase.inDoubt = true
	}
}

// MarkInDoubt marks an error as in doubt.
func (ase *AerospikeError) MarkInDoubt() {
	ase.inDoubt = true
}

// NewAerospikeError generates a new AerospikeError instance.
// If no message is provided, the result code will be translated into the default
// error message automatically.
// To be able to check for error type, you could use the following:
//   if aerr, ok := err.(AerospikeError); ok {
//       errCode := aerr.ResultCode()
//       errMessage := aerr.Error()
//   }
func NewAerospikeError(code ResultCode, messages ...string) error {
	if len(messages) == 0 {
		messages = []string{ResultCodeToString(code)}
	}

	err := errors.New(strings.Join(messages, " "))
	return AerospikeError{error: err, resultCode: code}
}

var ErrRecordsetClosed = NewAerospikeError(RECORDSET_CLOSED, "Recordset has already been closed.")
var ErrConnectionPoolEmpty = NewAerospikeError(NO_AVAILABLE_CONNECTIONS_TO_NODE)
