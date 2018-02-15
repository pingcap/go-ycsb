// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"fmt"
	"os"
)

// Fatalf prints the message and exits the program.
func Fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format, args...)
	fmt.Println("")
	os.Exit(1)
}

// Fatal prints the message and exits the program.
func Fatal(args ...interface{}) {
	fmt.Fprint(os.Stderr, args...)
	fmt.Println("")
	os.Exit(1)
}
