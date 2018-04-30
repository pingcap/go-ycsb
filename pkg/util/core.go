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

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
)

// CreateFieldIndices is a helper function to create a field -> index mapping
// for the core workload
func CreateFieldIndices(p *properties.Properties) map[string]int64 {
	fieldCount := p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
	m := make(map[string]int64, fieldCount)
	for i := int64(0); i < fieldCount; i++ {
		field := fmt.Sprintf("field%d", i)
		m[field] = i
	}
	return m
}

// AllFields is a helper function to create all fields
func AllFields(p *properties.Properties) []string {
	fieldCount := p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
	fields := make([]string, 0, fieldCount)
	for i := int64(0); i < fieldCount; i++ {
		field := fmt.Sprintf("field%d", i)
		fields = append(fields, field)
	}
	return fields
}
