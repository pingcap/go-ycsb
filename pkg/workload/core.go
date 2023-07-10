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

package workload

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/generator"
	"github.com/pingcap/go-ycsb/pkg/measurement"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type contextKey string

const StateKey = contextKey("core")

type CoreState struct {
	R *rand.Rand
	// FieldNames is a copy of Core.fieldNames to be goroutine-local
	FieldNames []string
}

type OperationType int64

const (
	read OperationType = iota + 1
	update
	insert
	scan
	readModifyWrite
)

// Core is the Core benchmark scenario. Represents a set of clients doing simple CRUD operations.
type Core struct {
	p *properties.Properties

	Table      string
	fieldCount int64
	fieldNames []string

	fieldLengthGenerator ycsb.Generator
	ReadAllFields        bool
	WriteAllFields       bool
	DataIntegrity        bool

	keySequence                  ycsb.Generator
	OperationChooser             *generator.Discrete
	keyChooser                   ycsb.Generator
	FieldChooser                 ycsb.Generator
	transactionInsertKeySequence *generator.AcknowledgedCounter
	scanLength                   ycsb.Generator
	orderedInserts               bool
	recordCount                  int64
	zeroPadding                  int64
	insertionRetryLimit          int64
	insertionRetryInterval       int64

	valuePool sync.Pool
}

func getFieldLengthGenerator(p *properties.Properties) ycsb.Generator {
	var fieldLengthGenerator ycsb.Generator
	fieldLengthDistribution := p.GetString(prop.FieldLengthDistribution, prop.FieldLengthDistributionDefault)
	fieldLength := p.GetInt64(prop.FieldLength, prop.FieldLengthDefault)
	fieldLengthHistogram := p.GetString(prop.FieldLengthHistogramFile, prop.FieldLengthHistogramFileDefault)

	switch strings.ToLower(fieldLengthDistribution) {
	case "constant":
		fieldLengthGenerator = generator.NewConstant(fieldLength)
	case "uniform":
		fieldLengthGenerator = generator.NewUniform(1, fieldLength)
	case "zipfian":
		fieldLengthGenerator = generator.NewZipfianWithRange(1, fieldLength, generator.ZipfianConstant)
	case "histogram":
		fieldLengthGenerator = generator.NewHistogramFromFile(fieldLengthHistogram)
	default:
		util.Fatalf("unknown field length distribution %s", fieldLengthDistribution)
	}

	return fieldLengthGenerator
}

func createOperationGenerator(p *properties.Properties) *generator.Discrete {
	readProportion := p.GetFloat64(prop.ReadProportion, prop.ReadProportionDefault)
	updateProportion := p.GetFloat64(prop.UpdateProportion, prop.UpdateProportionDefault)
	insertProportion := p.GetFloat64(prop.InsertProportion, prop.InsertProportionDefault)
	scanProportion := p.GetFloat64(prop.ScanProportion, prop.ScanProportionDefault)
	readModifyWriteProportion := p.GetFloat64(prop.ReadModifyWriteProportion, prop.ReadModifyWriteProportionDefault)
	OpNum = p.GetInt("opNum", 10)
	fmt.Printf("OpNum %d, read %f, write %f \n", OpNum, readProportion, updateProportion)
	operationChooser := generator.NewDiscrete()
	if readProportion > 0 {
		operationChooser.Add(readProportion, int64(read))
	}

	if updateProportion > 0 {
		operationChooser.Add(updateProportion, int64(update))
	}

	if insertProportion > 0 {
		operationChooser.Add(insertProportion, int64(insert))
	}

	if scanProportion > 0 {
		operationChooser.Add(scanProportion, int64(scan))
	}

	if readModifyWriteProportion > 0 {
		operationChooser.Add(readModifyWriteProportion, int64(readModifyWrite))
	}

	return operationChooser
}

// Load implements the Workload Load interface.
func (c *Core) Load(ctx context.Context, db ycsb.DB, totalCount int64) error {
	return nil
}

// InitThread implements the Workload InitThread interface.
func (c *Core) InitThread(ctx context.Context, _ int, _ int) context.Context {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	fieldNames := make([]string, len(c.fieldNames))
	copy(fieldNames, c.fieldNames)
	state := &CoreState{
		R:          r,
		FieldNames: fieldNames,
	}
	return context.WithValue(ctx, StateKey, state)
}

// CleanupThread implements the Workload CleanupThread interface.
func (c *Core) CleanupThread(_ context.Context) {

}

// Close implements the Workload Close interface.
func (c *Core) Close() error {
	return nil
}

func (c *Core) BuildKeyName(keyNum int64) string {
	if !c.orderedInserts {
		keyNum = util.Hash64(keyNum)
	}

	prefix := c.p.GetString(prop.KeyPrefix, prop.KeyPrefixDefault)
	return fmt.Sprintf("%s%0[3]*[2]d", prefix, keyNum, c.zeroPadding)
}

func (c *Core) BuildSingleValue(state *CoreState, key string) map[string][]byte {
	values := make(map[string][]byte, 1)

	r := state.R
	fieldKey := state.FieldNames[c.FieldChooser.Next(r)]

	var buf []byte
	if c.DataIntegrity {
		buf = c.buildDeterministicValue(state, key, fieldKey)
	} else {
		buf = c.buildRandomValue(state)
	}

	values[fieldKey] = buf

	return values
}

func (c *Core) BuildValues(state *CoreState, key string) map[string][]byte {
	values := make(map[string][]byte, c.fieldCount)

	for _, fieldKey := range state.FieldNames {
		var buf []byte
		if c.DataIntegrity {
			buf = c.buildDeterministicValue(state, key, fieldKey)
		} else {
			buf = c.buildRandomValue(state)
		}

		values[fieldKey] = buf
	}
	return values
}

func (c *Core) getValueBuffer(size int) []byte {
	buf := c.valuePool.Get().([]byte)
	if cap(buf) >= size {
		return buf[0:size]
	}

	return make([]byte, size)
}

func (c *Core) PutValues(values map[string][]byte) {
	for _, value := range values {
		c.valuePool.Put(value)
	}
}

func (c *Core) buildRandomValue(state *CoreState) []byte {
	// TODO: use pool for the buffer
	r := state.R
	buf := c.getValueBuffer(int(c.fieldLengthGenerator.Next(r)))
	util.RandBytes(r, buf)
	return buf
}

func (c *Core) buildDeterministicValue(state *CoreState, key string, fieldKey string) []byte {
	// TODO: use pool for the buffer
	r := state.R
	size := c.fieldLengthGenerator.Next(r)
	buf := c.getValueBuffer(int(size + 21))
	b := bytes.NewBuffer(buf[0:0])
	b.WriteString(key)
	b.WriteByte(':')
	b.WriteString(strings.ToLower(fieldKey))
	for int64(b.Len()) < size {
		b.WriteByte(':')
		n := util.BytesHash64(b.Bytes())
		b.WriteString(strconv.FormatUint(uint64(n), 10))
	}
	b.Truncate(int(size))
	return b.Bytes()
}

func (c *Core) VerifyRow(state *CoreState, key string, values map[string][]byte) {
	if len(values) == 0 {
		// null data here, need panic?
		return
	}

	for fieldKey, value := range values {
		expected := c.buildDeterministicValue(state, key, fieldKey)
		if !bytes.Equal(expected, value) {
			util.Fatalf("unexpected deterministic value, expect %q, but got %q", expected, value)
		}
	}
}

// DoInsert implements the Workload DoInsert interface.
func (c *Core) DoInsert(ctx context.Context, db ycsb.DB) error {
	state := ctx.Value(StateKey).(*CoreState)
	r := state.R
	keyNum := c.keySequence.Next(r)
	dbKey := c.BuildKeyName(keyNum)
	values := c.BuildValues(state, dbKey)
	defer c.PutValues(values)

	numOfRetries := int64(0)

	var err error
	for {
		err = db.Insert(ctx, c.Table, dbKey, values)
		if err == nil {
			break
		}

		select {
		case <-ctx.Done():
			if ctx.Err() == context.Canceled {
				return nil
			}
		default:
		}

		// Retry if configured. Without retrying, the load process will fail
		// even if one single insertion fails. User can optionally configure
		// an insertion retry limit (default is 0) to enable retry.
		numOfRetries++
		if numOfRetries > c.insertionRetryLimit {
			break
		}

		// Sleep for a random time betweensz [0.8, 1.2)*insertionRetryInterval
		sleepTimeMs := float64((c.insertionRetryInterval * 1000)) * (0.8 + 0.4*r.Float64())

		time.Sleep(time.Duration(sleepTimeMs) * time.Millisecond)
	}

	return err
}

// DoBatchInsert implements the Workload DoBatchInsert interface.
func (c *Core) DoBatchInsert(ctx context.Context, batchSize int, db ycsb.DB) error {
	batchDB, ok := db.(ycsb.BatchDB)
	if !ok {
		return fmt.Errorf("the %T does't implement the batchDB interface", db)
	}
	state := ctx.Value(StateKey).(*CoreState)
	r := state.R
	var keys []string
	var values []map[string][]byte
	for i := 0; i < batchSize; i++ {
		keyNum := c.keySequence.Next(r)
		dbKey := c.BuildKeyName(keyNum)
		keys = append(keys, dbKey)
		values = append(values, c.BuildValues(state, dbKey))
	}
	defer func() {
		for _, value := range values {
			c.PutValues(value)
		}
	}()

	numOfRetries := int64(0)
	var err error
	for {
		err = batchDB.BatchInsert(ctx, c.Table, keys, values)
		if err == nil {
			break
		}

		select {
		case <-ctx.Done():
			if ctx.Err() == context.Canceled {
				return nil
			}
		default:
		}

		// Retry if configured. Without retrying, the load process will fail
		// even if one single insertion fails. User can optionally configure
		// an insertion retry limit (default is 0) to enable retry.
		numOfRetries++
		if numOfRetries > c.insertionRetryLimit {
			break
		}

		// Sleep for a random time betweensz [0.8, 1.2)*insertionRetryInterval
		sleepTimeMs := float64((c.insertionRetryInterval * 1000)) * (0.8 + 0.4*r.Float64())

		time.Sleep(time.Duration(sleepTimeMs) * time.Millisecond)
	}
	return err
}

// DoTransaction implements the Workload DoTransaction interface.
func (c *Core) DoTransaction(ctx context.Context, db ycsb.DB) error {
	state := ctx.Value(StateKey).(*CoreState)
	r := state.R

	operation := OperationType(c.OperationChooser.Next(r))
	switch operation {
	case read:
		//return c.DoTransactionRead(ctx, db, state)
		return c.DoTransactionOps(ctx, db, state, read)
	case update:
		return c.DoTransactionOps(ctx, db, state, update)
		//return c.DoTransactionUpdate(ctx, db, state)
	case insert:
		return c.DoTransactionInsert(ctx, db, state)
	case scan:
		return c.DoTransactionScan(ctx, db, state)
	default:
		return c.DoTransactionReadModifyWrite(ctx, db, state)
	}
	return nil
}

var OpNum = 10

var TotalReadCounter uint64 = 0
var TotalUpdateCounter uint64 = 0

func (c *Core) DoTransactionOps(ctx context.Context, db ycsb.DB, state *CoreState, first_op OperationType) error {
	//fmt.Println("taas_tikv DoTransactionOps")
	keys := make([]string, OpNum)
	values := make([]map[string][]byte, OpNum)
	var readOpNum, writeOpNum uint64 = 0, 0
	keyNum1 := c.NextKeyNum(state)
	keyName1 := c.BuildKeyName(keyNum1)
	keys[0] = keyName1
	if first_op == read {
		values[0] = nil
		readOpNum++
		atomic.AddUint64(&TotalReadCounter, 1)
	} else {
		values[0] = c.BuildValues(state, keyName1)
		writeOpNum++
		atomic.AddUint64(&TotalUpdateCounter, 1)
	}
	for i := 1; i < OpNum; i++ {
		state1 := ctx.Value(StateKey).(*CoreState)
		r := state1.R
		operation := OperationType(c.OperationChooser.Next(r))
		switch operation {
		case read:
			keyNum := c.NextKeyNum(state1)
			keyName := c.BuildKeyName(keyNum)
			keys[i] = keyName
			values[i] = nil
			readOpNum++
			atomic.AddUint64(&TotalReadCounter, 1)
			break
		case update:
			keyNum := c.NextKeyNum(state1)
			keyName := c.BuildKeyName(keyNum)
			keys[i] = keyName
			values[i] = c.BuildValues(state1, keyName)
			writeOpNum++
			atomic.AddUint64(&TotalUpdateCounter, 1)
			break
		default:
			fmt.Printf("other operation type %d\n", operation)
			break
		}
	}

	return db.CommitToTaas(ctx, c.Table, keys, values)
}

// DoBatchTransaction implements the Workload DoBatchTransaction interface
func (c *Core) DoBatchTransaction(ctx context.Context, batchSize int, db ycsb.DB) error {
	batchDB, ok := db.(ycsb.BatchDB)
	if !ok {
		return fmt.Errorf("the %T does't implement the batchDB interface", db)
	}
	state := ctx.Value(StateKey).(*CoreState)
	r := state.R

	operation := OperationType(c.OperationChooser.Next(r))
	switch operation {
	case read:
		return c.doBatchTransactionRead(ctx, batchSize, batchDB, state)
	case insert:
		return c.doBatchTransactionInsert(ctx, batchSize, batchDB, state)
	case update:
		return c.doBatchTransactionUpdate(ctx, batchSize, batchDB, state)
	case scan:
		panic("The batch mode don't support the scan operation")
	default:
		return nil
	}
}

func (c *Core) NextKeyNum(state *CoreState) int64 {
	r := state.R
	keyNum := int64(0)
	if _, ok := c.keyChooser.(*generator.Exponential); ok {
		keyNum = -1
		for keyNum < 0 {
			keyNum = c.transactionInsertKeySequence.Last() - c.keyChooser.Next(r)
		}
	} else {
		keyNum = c.keyChooser.Next(r)
	}
	return keyNum
}

func (c *Core) DoTransactionRead(ctx context.Context, db ycsb.DB, state *CoreState) error {
	r := state.R
	keyNum := c.NextKeyNum(state)
	keyName := c.BuildKeyName(keyNum)

	var fields []string
	if !c.ReadAllFields {
		fieldName := state.FieldNames[c.FieldChooser.Next(r)]
		fields = append(fields, fieldName)
	} else {
		fields = state.FieldNames
	}

	values, err := db.Read(ctx, c.Table, keyName, fields)
	if err != nil {
		return err
	}

	if c.DataIntegrity {
		c.VerifyRow(state, keyName, values)
	}

	return nil
}

func (c *Core) DoTransactionReadModifyWrite(ctx context.Context, db ycsb.DB, state *CoreState) error {
	start := time.Now()
	defer func() {
		measurement.Measure("READ_MODIFY_WRITE", start, time.Now().Sub(start))
	}()

	r := state.R
	keyNum := c.NextKeyNum(state)
	keyName := c.BuildKeyName(keyNum)

	var fields []string
	if !c.ReadAllFields {
		fieldName := state.FieldNames[c.FieldChooser.Next(r)]
		fields = append(fields, fieldName)
	} else {
		fields = state.FieldNames
	}

	var values map[string][]byte
	if c.WriteAllFields {
		values = c.BuildValues(state, keyName)
	} else {
		values = c.BuildSingleValue(state, keyName)
	}
	defer c.PutValues(values)

	readValues, err := db.Read(ctx, c.Table, keyName, fields)
	if err != nil {
		return err
	}

	if err := db.Update(ctx, c.Table, keyName, values); err != nil {
		return err
	}

	if c.DataIntegrity {
		c.VerifyRow(state, keyName, readValues)
	}

	return nil
}

func (c *Core) DoTransactionInsert(ctx context.Context, db ycsb.DB, state *CoreState) error {
	r := state.R
	keyNum := c.transactionInsertKeySequence.Next(r)
	defer c.transactionInsertKeySequence.Acknowledge(keyNum)
	dbKey := c.BuildKeyName(keyNum)
	values := c.BuildValues(state, dbKey)
	defer c.PutValues(values)

	return db.Insert(ctx, c.Table, dbKey, values)
}

func (c *Core) DoTransactionScan(ctx context.Context, db ycsb.DB, state *CoreState) error {
	r := state.R
	keyNum := c.NextKeyNum(state)
	startKeyName := c.BuildKeyName(keyNum)

	scanLen := c.scanLength.Next(r)

	var fields []string
	if !c.ReadAllFields {
		fieldName := state.FieldNames[c.FieldChooser.Next(r)]
		fields = append(fields, fieldName)
	} else {
		fields = state.FieldNames
	}

	_, err := db.Scan(ctx, c.Table, startKeyName, int(scanLen), fields)

	return err
}

func (c *Core) DoTransactionUpdate(ctx context.Context, db ycsb.DB, state *CoreState) error {
	keyNum := c.NextKeyNum(state)
	keyName := c.BuildKeyName(keyNum)

	var values map[string][]byte
	if c.WriteAllFields {
		values = c.BuildValues(state, keyName)
	} else {
		values = c.BuildSingleValue(state, keyName)
	}

	defer c.PutValues(values)

	return db.Update(ctx, c.Table, keyName, values)
}

func (c *Core) doBatchTransactionRead(ctx context.Context, batchSize int, db ycsb.BatchDB, state *CoreState) error {
	r := state.R
	var fields []string

	if !c.ReadAllFields {
		fieldName := state.FieldNames[c.FieldChooser.Next(r)]
		fields = append(fields, fieldName)
	} else {
		fields = state.FieldNames
	}

	keys := make([]string, batchSize)
	for i := 0; i < batchSize; i++ {
		keys[i] = c.BuildKeyName(c.NextKeyNum(state))
	}

	_, err := db.BatchRead(ctx, c.Table, keys, fields)
	if err != nil {
		return err
	}

	// TODO should we verify the result?
	return nil
}

func (c *Core) doBatchTransactionInsert(ctx context.Context, batchSize int, db ycsb.BatchDB, state *CoreState) error {
	r := state.R
	keys := make([]string, batchSize)
	values := make([]map[string][]byte, batchSize)
	for i := 0; i < batchSize; i++ {
		keyNum := c.transactionInsertKeySequence.Next(r)
		keyName := c.BuildKeyName(keyNum)
		keys[i] = keyName
		if c.WriteAllFields {
			values[i] = c.BuildValues(state, keyName)
		} else {
			values[i] = c.BuildSingleValue(state, keyName)
		}
		c.transactionInsertKeySequence.Acknowledge(keyNum)
	}

	defer func() {
		for _, value := range values {
			c.PutValues(value)
		}
	}()

	return db.BatchInsert(ctx, c.Table, keys, values)
}

func (c *Core) doBatchTransactionUpdate(ctx context.Context, batchSize int, db ycsb.BatchDB, state *CoreState) error {
	keys := make([]string, batchSize)
	values := make([]map[string][]byte, batchSize)
	for i := 0; i < batchSize; i++ {
		keyNum := c.NextKeyNum(state)
		keyName := c.BuildKeyName(keyNum)
		keys[i] = keyName
		if c.WriteAllFields {
			values[i] = c.BuildValues(state, keyName)
		} else {
			values[i] = c.BuildSingleValue(state, keyName)
		}
	}

	defer func() {
		for _, value := range values {
			c.PutValues(value)
		}
	}()

	return db.BatchUpdate(ctx, c.Table, keys, values)
}

// CoreCreator creates the Core workload.
type coreCreator struct {
}

// Create implements the WorkloadCreator Create interface.
func (coreCreator) Create(p *properties.Properties) (ycsb.Workload, error) {
	c := new(Core)
	c.p = p
	c.Table = p.GetString(prop.TableName, prop.TableNameDefault)
	c.fieldCount = p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
	c.fieldNames = make([]string, c.fieldCount)
	for i := int64(0); i < c.fieldCount; i++ {
		c.fieldNames[i] = fmt.Sprintf("field%d", i)
	}
	c.fieldLengthGenerator = getFieldLengthGenerator(p)
	c.recordCount = p.GetInt64(prop.RecordCount, prop.RecordCountDefault)
	if c.recordCount == 0 {
		c.recordCount = int64(math.MaxInt32)
	}

	requestDistrib := p.GetString(prop.RequestDistribution, prop.RequestDistributionDefault)
	maxScanLength := p.GetInt64(prop.MaxScanLength, prop.MaxScanLengthDefault)
	scanLengthDistrib := p.GetString(prop.ScanLengthDistribution, prop.ScanLengthDistributionDefault)

	insertStart := p.GetInt64(prop.InsertStart, prop.InsertStartDefault)
	insertCount := p.GetInt64(prop.InsertCount, c.recordCount-insertStart)
	if c.recordCount < insertStart+insertCount {
		util.Fatalf("record count %d must be bigger than insert start %d + count %d",
			c.recordCount, insertStart, insertCount)
	}
	c.zeroPadding = p.GetInt64(prop.ZeroPadding, prop.ZeroPaddingDefault)
	c.ReadAllFields = p.GetBool(prop.ReadAllFields, prop.ReadALlFieldsDefault)
	c.WriteAllFields = p.GetBool(prop.WriteAllFields, prop.WriteAllFieldsDefault)
	c.DataIntegrity = p.GetBool(prop.DataIntegrity, prop.DataIntegrityDefault)
	fieldLengthDistribution := p.GetString(prop.FieldLengthDistribution, prop.FieldLengthDistributionDefault)
	if c.DataIntegrity && fieldLengthDistribution != "constant" {
		util.Fatal("must have constant field size to check data integrity")
	}

	if p.GetString(prop.InsertOrder, prop.InsertOrderDefault) == "hashed" {
		c.orderedInserts = false
	} else {
		c.orderedInserts = true
	}

	c.keySequence = generator.NewCounter(insertStart)
	c.OperationChooser = createOperationGenerator(p)
	var keyrangeLowerBound int64 = insertStart
	var keyrangeUpperBound int64 = insertStart + insertCount - 1

	c.transactionInsertKeySequence = generator.NewAcknowledgedCounter(c.recordCount)
	switch requestDistrib {
	case "uniform":
		c.keyChooser = generator.NewUniform(keyrangeLowerBound, keyrangeUpperBound)
	case "sequential":
		c.keyChooser = generator.NewSequential(keyrangeLowerBound, keyrangeUpperBound)
	case "zipfian":
		insertProportion := p.GetFloat64(prop.InsertProportion, prop.InsertProportionDefault)
		opCount := p.GetInt64(prop.OperationCount, 0)
		expectedNewKeys := int64(float64(opCount) * insertProportion * 2.0)
		keyrangeUpperBound = insertStart + insertCount + expectedNewKeys
		c.keyChooser = generator.NewScrambledZipfian(keyrangeLowerBound, keyrangeUpperBound, generator.ZipfianConstant)
	case "latest":
		c.keyChooser = generator.NewSkewedLatest(c.transactionInsertKeySequence)
	case "hotspot":
		hotsetFraction := p.GetFloat64(prop.HotspotDataFraction, prop.HotspotDataFractionDefault)
		hotopnFraction := p.GetFloat64(prop.HotspotOpnFraction, prop.HotspotOpnFractionDefault)
		c.keyChooser = generator.NewHotspot(keyrangeLowerBound, keyrangeUpperBound, hotsetFraction, hotopnFraction)
	case "exponential":
		percentile := p.GetFloat64(prop.ExponentialPercentile, prop.ExponentialPercentileDefault)
		frac := p.GetFloat64(prop.ExponentialFrac, prop.ExponentialFracDefault)
		c.keyChooser = generator.NewExponential(percentile, float64(c.recordCount)*frac)
	default:
		util.Fatalf("unknown request distribution %s", requestDistrib)
	}
	fmt.Println(fmt.Sprintf("Using request distribution '%s' a keyrange of [%d %d]", requestDistrib, keyrangeLowerBound, keyrangeUpperBound))

	c.FieldChooser = generator.NewUniform(0, c.fieldCount-1)
	switch scanLengthDistrib {
	case "uniform":
		c.scanLength = generator.NewUniform(1, maxScanLength)
	case "zipfian":
		c.scanLength = generator.NewZipfianWithRange(1, maxScanLength, generator.ZipfianConstant)
	default:
		util.Fatalf("distribution %s not allowed for scan length", scanLengthDistrib)
	}

	c.insertionRetryLimit = p.GetInt64(prop.InsertionRetryLimit, prop.InsertionRetryLimitDefault)
	c.insertionRetryInterval = p.GetInt64(prop.InsertionRetryInterval, prop.InsertionRetryIntervalDefault)

	fieldLength := p.GetInt64(prop.FieldLength, prop.FieldLengthDefault)
	c.valuePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, fieldLength)
		},
	}

	return c, nil
}

func init() {
	ycsb.RegisterWorkloadCreator("core", coreCreator{})
	ycsb.RegisterWorkloadCreator("site.ycsb.workloads.CoreWorkload", coreCreator{})
}
