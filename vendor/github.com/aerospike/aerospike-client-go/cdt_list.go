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

// List bin operations. Create list operations used by client.Operate command.
// List operations support negative indexing.  If the index is negative, the
// resolved index starts backwards from end of list.
//
// Index/Range examples:
//
//    Index 0: First item in list.
//    Index 4: Fifth item in list.
//    Index -1: Last item in list.
//    Index -3: Third to last item in list.
//    Index 1 Count 2: Second and third items in list.
//    Index -3 Count 3: Last three items in list.
//    Index -5 Count 4: Range between fifth to last item to second to last item inclusive.
//
// If an index is out of bounds, a parameter error will be returned. If a range is partially
// out of bounds, the valid part of the range will be returned.

const (
	_CDT_LIST_SET_TYPE                 = 0
	_CDT_LIST_APPEND                   = 1
	_CDT_LIST_APPEND_ITEMS             = 2
	_CDT_LIST_INSERT                   = 3
	_CDT_LIST_INSERT_ITEMS             = 4
	_CDT_LIST_POP                      = 5
	_CDT_LIST_POP_RANGE                = 6
	_CDT_LIST_REMOVE                   = 7
	_CDT_LIST_REMOVE_RANGE             = 8
	_CDT_LIST_SET                      = 9
	_CDT_LIST_TRIM                     = 10
	_CDT_LIST_CLEAR                    = 11
	_CDT_LIST_INCREMENT                = 12
	_CDT_LIST_SORT                     = 13
	_CDT_LIST_SIZE                     = 16
	_CDT_LIST_GET                      = 17
	_CDT_LIST_GET_RANGE                = 18
	_CDT_LIST_GET_BY_INDEX             = 19
	_CDT_LIST_GET_BY_RANK              = 21
	_CDT_LIST_GET_BY_VALUE             = 22
	_CDT_LIST_GET_BY_VALUE_LIST        = 23
	_CDT_LIST_GET_BY_INDEX_RANGE       = 24
	_CDT_LIST_GET_BY_VALUE_INTERVAL    = 25
	_CDT_LIST_GET_BY_RANK_RANGE        = 26
	_CDT_LIST_REMOVE_BY_INDEX          = 32
	_CDT_LIST_REMOVE_BY_RANK           = 34
	_CDT_LIST_REMOVE_BY_VALUE          = 35
	_CDT_LIST_REMOVE_BY_VALUE_LIST     = 36
	_CDT_LIST_REMOVE_BY_INDEX_RANGE    = 37
	_CDT_LIST_REMOVE_BY_VALUE_INTERVAL = 38
	_CDT_LIST_REMOVE_BY_RANK_RANGE     = 39
)

type ListOrderType int

// Map storage order.
const (
	// ListOrderUnordered signifies that list is not ordered. This is the default.
	ListOrderUnordered ListOrderType = 0

	// ListOrderOrdered signifies that list is Ordered.
	ListOrderOrdered ListOrderType = 1
)

// ListPolicy directives when creating a list and writing list items.
type ListPolicy struct {
	attributes ListOrderType
	flags      int
}

// Create unique key map with specified order when map does not exist.
// Use specified write mode when writing map items.
func NewListPolicy(order ListOrderType, flags int) *ListPolicy {
	return &ListPolicy{
		attributes: order,
		flags:      flags,
	}
}

// DefaultListPolicy is the default list policy and can be customized.
var defaultListPolicy = NewListPolicy(ListOrderUnordered, ListWriteFlagsDefault)

func DefaultListPolicy() *ListPolicy {
	return defaultListPolicy
}

type ListReturnType int

const (
	// Do not return a result.
	ListReturnTypeNone ListReturnType = 0

	// Return index offset order.
	// 0 = first key
	// N = Nth key
	// -1 = last key
	ListReturnTypeIndex ListReturnType = 1

	// Return reverse index offset order.
	// 0 = last key
	// -1 = first key
	ListReturnTypeReverseIndex ListReturnType = 2

	// Return value order.
	// 0 = smallest value
	// N = Nth smallest value
	// -1 = largest value
	ListReturnTypeRank ListReturnType = 3

	// Return reserve value order.
	// 0 = largest value
	// N = Nth largest value
	// -1 = smallest value
	ListReturnTypeReverseRank ListReturnType = 4

	// Return count of items selected.
	ListReturnTypeCount ListReturnType = 5

	// Return value for single key read and value list for range read.
	ListReturnTypeValue ListReturnType = 7

	// Invert meaning of list command and return values.  For example:
	// ListOperation.getByIndexRange(binName, index, count, ListReturnType.INDEX | ListReturnType.INVERTED)
	// With the INVERTED flag enabled, the items outside of the specified index range will be returned.
	// The meaning of the list command can also be inverted.  For example:
	// ListOperation.removeByIndexRange(binName, index, count, ListReturnType.INDEX | ListReturnType.INVERTED);
	// With the INVERTED flag enabled, the items outside of the specified index range will be removed and returned.
	ListReturnTypeInverted ListReturnType = 0x10000
)

// ListSortFlags detemines sort flags for CDT lists
// type ListSortFlags int

const (
	ListSortFlagsDefault        = 0
	ListSortFlagsDropDuplicates = 2
)

// ListWriteFlags detemines write flags for CDT lists
// type ListWriteFlags int

const (
	ListWriteFlagsDefault       = 0
	ListWriteFlagsAddUnique     = 1
	ListWriteFlagsInsertBounded = 2
)

func listGenericOpEncoder(op *Operation, packer BufferEx) (int, error) {
	args := op.binValue.(ListValue)
	if len(args) > 1 {
		return packCDTIfcVarParamsAsArray(packer, int16(args[0].(int)), args[1:]...)
	}
	return packCDTIfcVarParamsAsArray(packer, int16(args[0].(int)))
}

func packCDTParamsAsArray(packer BufferEx, opType int16, params ...Value) (int, error) {
	size := 0
	n, err := __PackShortRaw(packer, opType)
	if err != nil {
		return n, err
	}
	size += n

	if len(params) > 0 {
		if n, err = __PackArrayBegin(packer, len(params)); err != nil {
			return size + n, err
		}
		size += n

		for i := range params {
			if n, err = params[i].pack(packer); err != nil {
				return size + n, err
			}
			size += n
		}
	}
	return size, nil
}

func packCDTIfcParamsAsArray(packer BufferEx, opType int16, params ListValue) (int, error) {
	return packCDTIfcVarParamsAsArray(packer, opType, []interface{}(params)...)
}

func packCDTIfcVarParamsAsArray(packer BufferEx, opType int16, params ...interface{}) (int, error) {
	size := 0
	n, err := __PackShortRaw(packer, opType)
	if err != nil {
		return n, err
	}
	size += n

	if len(params) > 0 {
		if n, err = __PackArrayBegin(packer, len(params)); err != nil {
			return size + n, err
		}
		size += n

		for i := range params {
			if n, err = __PackObject(packer, params[i], false); err != nil {
				return size + n, err
			}
			size += n
		}
	}
	return size, nil
}

// ListSetOrderOp creates a set list order operation.
// Server sets list order.  Server returns null.
func ListSetOrderOp(binName string, listOrder ListOrderType) *Operation {
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_SET_TYPE, IntegerValue(listOrder)}, encoder: listGenericOpEncoder}
}

// ListAppendOp creates a list append operation.
// Server appends values to end of list bin.
// Server returns list size on bin name.
// It will panic is no values have been passed.
func ListAppendOp(binName string, values ...interface{}) *Operation {
	if len(values) == 1 {
		return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_APPEND, NewValue(values[0])}, encoder: listGenericOpEncoder}
	}
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_APPEND_ITEMS, ListValue(values)}, encoder: listGenericOpEncoder}
}

// ListAppendWithPolicyOp creates a list append operation.
// Server appends values to end of list bin.
// Server returns list size on bin name.
// It will panic is no values have been passed.
func ListAppendWithPolicyOp(policy *ListPolicy, binName string, values ...interface{}) *Operation {
	switch len(values) {
	case 1:
		return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_APPEND, NewValue(values[0]), IntegerValue(policy.attributes), IntegerValue(policy.flags)}, encoder: listGenericOpEncoder}
	default:
		return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_APPEND_ITEMS, ListValue(values), IntegerValue(policy.attributes), IntegerValue(policy.flags)}, encoder: listGenericOpEncoder}
	}
}

// ListInsertOp creates a list insert operation.
// Server inserts value to specified index of list bin.
// Server returns list size on bin name.
// It will panic is no values have been passed.
func ListInsertOp(binName string, index int, values ...interface{}) *Operation {
	if len(values) == 1 {
		return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_INSERT, IntegerValue(index), NewValue(values[0])}, encoder: listGenericOpEncoder}
	}
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_INSERT_ITEMS, IntegerValue(index), ListValue(values)}, encoder: listGenericOpEncoder}
}

// ListInsertWithPolicyOp creates a list insert operation.
// Server inserts value to specified index of list bin.
// Server returns list size on bin name.
// It will panic is no values have been passed.
func ListInsertWithPolicyOp(policy *ListPolicy, binName string, index int, values ...interface{}) *Operation {
	if len(values) == 1 {
		return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_INSERT, IntegerValue(index), NewValue(values[0]), IntegerValue(policy.flags)}, encoder: listGenericOpEncoder}
	}
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_INSERT_ITEMS, IntegerValue(index), ListValue(values), IntegerValue(policy.flags)}, encoder: listGenericOpEncoder}
}

// ListPopOp creates list pop operation.
// Server returns item at specified index and removes item from list bin.
func ListPopOp(binName string, index int) *Operation {
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_POP, IntegerValue(index)}, encoder: listGenericOpEncoder}
}

// ListPopRangeOp creates a list pop range operation.
// Server returns items starting at specified index and removes items from list bin.
func ListPopRangeOp(binName string, index int, count int) *Operation {
	if count == 1 {
		return ListPopOp(binName, index)
	}

	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_POP_RANGE, IntegerValue(index), IntegerValue(count)}, encoder: listGenericOpEncoder}
}

// ListPopRangeFromOp creates a list pop range operation.
// Server returns items starting at specified index to the end of list and removes items from list bin.
func ListPopRangeFromOp(binName string, index int) *Operation {
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_POP_RANGE, IntegerValue(index)}, encoder: listGenericOpEncoder}
}

// ListRemoveOp creates a list remove operation.
// Server removes item at specified index from list bin.
// Server returns number of items removed.
func ListRemoveOp(binName string, index int) *Operation {
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_REMOVE, IntegerValue(index)}, encoder: listGenericOpEncoder}
}

// ListRemoveByValueOp creates list remove by value operation.
// Server removes the item identified by value and returns removed data specified by returnType.
func ListRemoveByValueOp(binName string, value interface{}, returnType ListReturnType) *Operation {
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_REMOVE_BY_VALUE, IntegerValue(returnType), NewValue(value)}, encoder: listGenericOpEncoder}
}

// ListRemoveByValueListOp creates list remove by value operation.
// Server removes list items identified by value and returns removed data specified by returnType.
func ListRemoveByValueListOp(binName string, values []interface{}, returnType ListReturnType) *Operation {
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_REMOVE_BY_VALUE_LIST, IntegerValue(returnType), ListValue(values)}, encoder: listGenericOpEncoder}
}

// ListRemoveByValueRangeOp creates a list remove operation.
// Server removes list items identified by value range (valueBegin inclusive, valueEnd exclusive).
// If valueBegin is nil, the range is less than valueEnd.
// If valueEnd is nil, the range is greater than equal to valueBegin.
// Server returns removed data specified by returnType
func ListRemoveByValueRangeOp(binName string, returnType ListReturnType, valueBegin, valueEnd interface{}) *Operation {
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_REMOVE_BY_VALUE_INTERVAL, IntegerValue(returnType), NewValue(valueBegin), NewValue(valueEnd)}, encoder: listGenericOpEncoder}
}

// ListRemoveRangeOp creates a list remove range operation.
// Server removes "count" items starting at specified index from list bin.
// Server returns number of items removed.
func ListRemoveRangeOp(binName string, index int, count int) *Operation {
	if count == 1 {
		return ListRemoveOp(binName, index)
	}

	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_REMOVE_RANGE, IntegerValue(index), IntegerValue(count)}, encoder: listGenericOpEncoder}
}

// ListRemoveRangeFromOp creates a list remove range operation.
// Server removes all items starting at specified index to the end of list.
// Server returns number of items removed.
func ListRemoveRangeFromOp(binName string, index int) *Operation {
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_REMOVE_RANGE, IntegerValue(index)}, encoder: listGenericOpEncoder}
}

// ListSetOp creates a list set operation.
// Server sets item value at specified index in list bin.
// Server does not return a result by default.
func ListSetOp(binName string, index int, value interface{}) *Operation {
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_SET, IntegerValue(index), NewValue(value)}, encoder: listGenericOpEncoder}
}

// ListTrimOp creates a list trim operation.
// Server removes items in list bin that do not fall into range specified by index
// and count range. If the range is out of bounds, then all items will be removed.
// Server returns number of elemts that were removed.
func ListTrimOp(binName string, index int, count int) *Operation {
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_TRIM, IntegerValue(index), IntegerValue(count)}, encoder: listGenericOpEncoder}
}

// ListClearOp creates a list clear operation.
// Server removes all items in list bin.
// Server does not return a result by default.
func ListClearOp(binName string) *Operation {
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_CLEAR}, encoder: listGenericOpEncoder}
}

// ListIncrementOp creates a list increment operation.
// Server increments list[index] by value.
// Value should be integer(IntegerValue, LongValue) or float(FloatValue).
// Server returns list[index] after incrementing.
func ListIncrementOp(binName string, index int, value interface{}) *Operation {
	val := NewValue(value)
	switch val.(type) {
	case LongValue, IntegerValue, FloatValue:
	default:
		panic("Increment operation only accepts Integer or Float values")
	}
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_INCREMENT, IntegerValue(index), NewValue(value)}, encoder: listGenericOpEncoder}
}

// ListIncrementOp creates list increment operation with policy.
// Server increments list[index] by 1.
// Server returns list[index] after incrementing.
func ListIncrementByOneOp(binName string, index int) *Operation {
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_INCREMENT, IntegerValue(index)}, encoder: listGenericOpEncoder}
}

// ListIncrementByOneWithPolicyOp creates list increment operation with policy.
// Server increments list[index] by 1.
// Server returns list[index] after incrementing.
func ListIncrementByOneWithPolicyOp(policy *ListPolicy, binName string, index int) *Operation {
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_INCREMENT, IntegerValue(index), IntegerValue(1), IntegerValue(policy.attributes), IntegerValue(policy.flags)}, encoder: listGenericOpEncoder}
}

// ListInsertWithPolicyOp creates a list insert operation.
// Server inserts value to specified index of list bin.
// Server returns list size on bin name.
// It will panic is no values have been passed.
func ListIncrementWithPolicyOp(policy *ListPolicy, binName string, index int, value interface{}) *Operation {
	val := NewValue(value)
	switch val.(type) {
	case LongValue, IntegerValue, FloatValue:
	default:
		panic("Increment operation only accepts Integer or Float values")
	}
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_INCREMENT, IntegerValue(index), NewValue(value), IntegerValue(policy.flags)}, encoder: listGenericOpEncoder}
}

// ListSizeOp creates a list size operation.
// Server returns size of list on bin name.
func ListSizeOp(binName string) *Operation {
	return &Operation{opType: CDT_READ, binName: binName, binValue: ListValue{_CDT_LIST_SIZE}, encoder: listGenericOpEncoder}
}

// ListGetOp creates a list get operation.
// Server returns item at specified index in list bin.
func ListGetOp(binName string, index int) *Operation {
	return &Operation{opType: CDT_READ, binName: binName, binValue: ListValue{_CDT_LIST_GET, IntegerValue(index)}, encoder: listGenericOpEncoder}
}

// ListGetRangeOp creates a list get range operation.
// Server returns "count" items starting at specified index in list bin.
func ListGetRangeOp(binName string, index int, count int) *Operation {
	return &Operation{opType: CDT_READ, binName: binName, binValue: ListValue{_CDT_LIST_GET_RANGE, IntegerValue(index), IntegerValue(count)}, encoder: listGenericOpEncoder}
}

// ListGetRangeFromOp creates a list get range operation.
// Server returns items starting at specified index to the end of list.
func ListGetRangeFromOp(binName string, index int) *Operation {
	return &Operation{opType: CDT_READ, binName: binName, binValue: ListValue{_CDT_LIST_GET_RANGE, IntegerValue(index)}, encoder: listGenericOpEncoder}
}

// ListSortOp creates list sort operation.
// Server sorts list according to sortFlags.
// Server does not return a result by default.
func ListSortOp(binName string, sortFlags int) *Operation {
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_SORT, IntegerValue(sortFlags)}, encoder: listGenericOpEncoder}
}

// ListRemoveByIndexOp creates a list remove operation.
// Server removes list item identified by index and returns removed data specified by returnType.
func ListRemoveByIndexOp(binName string, index int, returnType ListReturnType) *Operation {
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_REMOVE_BY_INDEX, IntegerValue(returnType), IntegerValue(index)}, encoder: listGenericOpEncoder}
}

// ListRemoveByIndexRangeOp creates a list remove operation.
// Server removes list items starting at specified index to the end of list and returns removed
// data specified by returnType.
func ListRemoveByIndexRangeOp(binName string, index, returnType ListReturnType) *Operation {
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_REMOVE_BY_INDEX_RANGE, IntegerValue(returnType), IntegerValue(index)}, encoder: listGenericOpEncoder}
}

// ListRemoveByIndexRangeCountOp creates a list remove operation.
// Server removes "count" list items starting at specified index and returns removed data specified by returnType.
func ListRemoveByIndexRangeCountOp(binName string, index, count int, returnType ListReturnType) *Operation {
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_REMOVE_BY_INDEX_RANGE, IntegerValue(returnType), IntegerValue(index), IntegerValue(count)}, encoder: listGenericOpEncoder}
}

// ListRemoveByRankOp creates a list remove operation.
// Server removes list item identified by rank and returns removed data specified by returnType.
func ListRemoveByRankOp(binName string, rank int, returnType ListReturnType) *Operation {
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_REMOVE_BY_RANK, IntegerValue(returnType), IntegerValue(rank)}, encoder: listGenericOpEncoder}
}

// ListRemoveByRankRangeOp creates a list remove operation.
// Server removes list items starting at specified rank to the last ranked item and returns removed
// data specified by returnType.
func ListRemoveByRankRangeOp(binName string, rank int, returnType ListReturnType) *Operation {
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_REMOVE_BY_RANK_RANGE, IntegerValue(returnType), IntegerValue(rank)}, encoder: listGenericOpEncoder}
}

// ListRemoveByRankRangeCountOp creates a list remove operation.
// Server removes "count" list items starting at specified rank and returns removed data specified by returnType.
func ListRemoveByRankRangeCountOp(binName string, rank int, count int, returnType ListReturnType) *Operation {
	return &Operation{opType: CDT_MODIFY, binName: binName, binValue: ListValue{_CDT_LIST_REMOVE_BY_RANK_RANGE, IntegerValue(returnType), IntegerValue(rank), IntegerValue(count)}, encoder: listGenericOpEncoder}
}

// ListGetByValueOp creates a list get by value operation.
// Server selects list items identified by value and returns selected data specified by returnType.
func ListGetByValueOp(binName string, value interface{}, returnType ListReturnType) *Operation {
	return &Operation{opType: CDT_READ, binName: binName, binValue: ListValue{_CDT_LIST_GET_BY_VALUE, IntegerValue(returnType), NewValue(value)}, encoder: listGenericOpEncoder}
}

// ListGetByValueListOp creates list get by value list operation.
// Server selects list items identified by values and returns selected data specified by returnType.
func ListGetByValueListOp(binName string, values []interface{}, returnType ListReturnType) *Operation {
	return &Operation{opType: CDT_READ, binName: binName, binValue: ListValue{_CDT_LIST_GET_BY_VALUE_LIST, IntegerValue(returnType), ListValue(values)}, encoder: listGenericOpEncoder}
}

// ListGetByValueRangeOp creates a list get by value range operation.
// Server selects list items identified by value range (valueBegin inclusive, valueEnd exclusive)
// If valueBegin is null, the range is less than valueEnd.
// If valueEnd is null, the range is greater than equal to valueBegin.
// Server returns selected data specified by returnType.
func ListGetByValueRangeOp(binName string, beginValue, endValue interface{}, returnType ListReturnType) *Operation {
	return &Operation{opType: CDT_READ, binName: binName, binValue: ListValue{_CDT_LIST_GET_BY_VALUE_INTERVAL, IntegerValue(returnType), NewValue(beginValue), NewValue(endValue)}, encoder: listGenericOpEncoder}
}

// ListGetByIndexOp creates list get by index operation.
// Server selects list item identified by index and returns selected data specified by returnType
func ListGetByIndexOp(binName string, index int, returnType ListReturnType) *Operation {
	return &Operation{opType: CDT_READ, binName: binName, binValue: ListValue{_CDT_LIST_GET_BY_INDEX, IntegerValue(returnType), IntegerValue(index)}, encoder: listGenericOpEncoder}
}

// ListGetByIndexRangeOp creates list get by index range operation.
// Server selects list items starting at specified index to the end of list and returns selected
// data specified by returnType.
func ListGetByIndexRangeOp(binName string, index int, returnType ListReturnType) *Operation {
	return &Operation{opType: CDT_READ, binName: binName, binValue: ListValue{_CDT_LIST_GET_BY_INDEX_RANGE, IntegerValue(returnType), IntegerValue(index)}, encoder: listGenericOpEncoder}
}

// ListGetByIndexRangeCountOp creates list get by index range operation.
// Server selects "count" list items starting at specified index and returns selected data specified
// by returnType.
func ListGetByIndexRangeCountOp(binName string, index, count int, returnType ListReturnType) *Operation {
	return &Operation{opType: CDT_READ, binName: binName, binValue: ListValue{_CDT_LIST_GET_BY_INDEX_RANGE, IntegerValue(returnType), IntegerValue(index), count}, encoder: listGenericOpEncoder}
}

// ListGetByRankOp creates a list get by rank operation.
// Server selects list item identified by rank and returns selected data specified by returnType.
func ListGetByRankOp(binName string, rank int, returnType ListReturnType) *Operation {
	return &Operation{opType: CDT_READ, binName: binName, binValue: ListValue{_CDT_LIST_GET_BY_RANK, IntegerValue(returnType), IntegerValue(rank)}, encoder: listGenericOpEncoder}
}

// ListGetByRankRangeOp creates a list get by rank range operation.
// Server selects list items starting at specified rank to the last ranked item and returns selected
// data specified by returnType.
func ListGetByRankRangeOp(binName string, rank int, returnType ListReturnType) *Operation {
	return &Operation{opType: CDT_READ, binName: binName, binValue: ListValue{_CDT_LIST_GET_BY_RANK_RANGE, IntegerValue(returnType), IntegerValue(rank)}, encoder: listGenericOpEncoder}
}

// ListGetByRankRangeCountOp creates a list get by rank range operation.
// Server selects "count" list items starting at specified rank and returns selected data specified by returnType.
func ListGetByRankRangeCountOp(binName string, rank, count int, returnType ListReturnType) *Operation {
	return &Operation{opType: CDT_READ, binName: binName, binValue: ListValue{_CDT_LIST_GET_BY_RANK_RANGE, IntegerValue(returnType), IntegerValue(rank), IntegerValue(count)}, encoder: listGenericOpEncoder}
}
