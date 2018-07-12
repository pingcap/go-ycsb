// Copyright 2013-2017 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package aerospike

func newCDTCreateOperationEncoder(op *Operation, packer BufferEx) (int, error) {
	if op.binValue != nil {
		if params := op.binValue.(ListValue); len(params) > 0 {
			return packCDTIfcParamsAsArray(packer, int16(*op.opSubType), op.binValue.(ListValue))
		}
	}
	return packCDTParamsAsArray(packer, int16(*op.opSubType))
}

func newCDTCreateOperationValues2(command int, attributes mapOrderType, binName string, value1 interface{}, value2 interface{}) *Operation {
	return &Operation{
		opType:    MAP_MODIFY,
		opSubType: &command,
		binName:   binName,
		binValue:  ListValue([]interface{}{value1, value2, IntegerValue(attributes)}),
		encoder:   newCDTCreateOperationEncoder,
	}
}

func newCDTCreateOperationValues0(command int, typ OperationType, binName string) *Operation {
	return &Operation{
		opType:    typ,
		opSubType: &command,
		binName:   binName,
		// binValue: NewNullValue(),
		encoder: newCDTCreateOperationEncoder,
	}
}

func newCDTCreateOperationValuesN(command int, typ OperationType, binName string, values []interface{}, returnType mapReturnType) *Operation {
	return &Operation{
		opType:    typ,
		opSubType: &command,
		binName:   binName,
		binValue:  ListValue([]interface{}{IntegerValue(returnType), ListValue(values)}),
		encoder:   newCDTCreateOperationEncoder,
	}
}

func newCDTCreateOperationValue1(command int, typ OperationType, binName string, value interface{}, returnType mapReturnType) *Operation {
	return &Operation{
		opType:    typ,
		opSubType: &command,
		binName:   binName,
		binValue:  ListValue([]interface{}{IntegerValue(returnType), value}),
		encoder:   newCDTCreateOperationEncoder,
	}
}

func newCDTCreateOperationIndex(command int, typ OperationType, binName string, index int, returnType mapReturnType) *Operation {
	return &Operation{
		opType:    typ,
		opSubType: &command,
		binName:   binName,
		binValue:  ListValue([]interface{}{IntegerValue(returnType), index}),
		encoder:   newCDTCreateOperationEncoder,
	}
}

func newCDTCreateOperationIndexCount(command int, typ OperationType, binName string, index int, count int, returnType mapReturnType) *Operation {
	return &Operation{
		opType:    typ,
		opSubType: &command,
		binName:   binName,
		binValue:  ListValue([]interface{}{IntegerValue(returnType), index, count}),
		encoder:   newCDTCreateOperationEncoder,
	}
}

func newCDTCreateRangeOperation(command int, typ OperationType, binName string, begin interface{}, end interface{}, returnType mapReturnType) *Operation {
	if end == nil {
		return &Operation{
			opType:    typ,
			opSubType: &command,
			binName:   binName,
			binValue:  ListValue([]interface{}{IntegerValue(returnType), begin}),
			encoder:   newCDTCreateOperationEncoder,
		}
	}

	return &Operation{
		opType:    typ,
		opSubType: &command,
		binName:   binName,
		binValue:  ListValue([]interface{}{IntegerValue(returnType), begin, end}),
		encoder:   newCDTCreateOperationEncoder,
	}
}
