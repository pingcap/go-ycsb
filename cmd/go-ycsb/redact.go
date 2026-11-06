// Copyright 2026 PingCAP, Inc.
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

package main

import "strings"

var credentialKeyNormalizer = strings.NewReplacer("-", "", "_", "")

// redactPropertyValue hides credentials in property summaries without changing
// the values passed to database drivers.
func redactPropertyValue(key, value string) string {
	name := strings.ToLower(credentialKeyNormalizer.Replace(key))
	for _, marker := range []string{"password", "passwd", "secret", "token", "accesskey", "apikey"} {
		if strings.Contains(name, marker) {
			return "[REDACTED]"
		}
	}
	// Also cover malformed URLs and comma-separated endpoint lists, which may
	// contain credentials even when a URL parser rejects them.
	if strings.Contains(value, "://") && strings.Contains(value, "@") {
		return "[REDACTED]"
	}
	return value
}
