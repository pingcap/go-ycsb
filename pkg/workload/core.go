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
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/client"
	"github.com/pingcap/go-ycsb/pkg/generator"
	"github.com/pingcap/go-ycsb/pkg/measurement"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type contextKey string

const stateKey = contextKey("core")

type coreState struct {
	r *rand.Rand
	// fieldNames is a copy of core.fieldNames to be goroutine-local
	fieldNames   []string
	curValueSize int64
}

type operationType int64

const (
	read operationType = iota + 1
	update
	insert
	scan
	readModifyWrite
)

// Core is the core benchmark scenario. Represents a set of clients doing simple CRUD operations.
type core struct {
	p *properties.Properties

	table      string
	fieldCount int64
	fieldNames []string

	fieldLengthGenerator ycsb.Generator
	readAllFields        bool
	writeAllFields       bool
	dataIntegrity        bool

	keySequence                  ycsb.Generator
	operationChooser             *generator.Discrete
	keyChooser                   ycsb.Generator
	fieldChooser                 ycsb.Generator
	transactionInsertKeySequence *generator.AcknowledgedCounter
	scanLength                   ycsb.Generator
	orderedInserts               bool
	recordCount                  int64
	zeroPadding                  int64
	insertionRetryLimit          int64
	insertionRetryInterval       int64
	fieldLength                  int64
	maxScanLength                int64

	valuePool sync.Pool

	runMixGraphBench  bool
	useDefaultRequest bool
	variedSize        bool
	queryChooser      ycsb.Generator
	genExp            *generator.TwoTermExpKeys
	usePrefixModeling bool
	keyDistA          float64
	keyDistB          float64
	valueTheta        float64
	valueK            float64
	valueSigma        float64
	iterTheta         float64
	iterK             float64
	iterSigma         float64
	keyRangeNum       int64
	avgValueSizes     []int64
	keyRangeFreqs     []int64
	keyRangeSumVSize  []int64
	keyRangeSize      int64

	traceReplay bool

	nextShuffleRegionTime       time.Time
	shuffleRegionInterval       uint64
	needShuffleRegion           bool
	kvSizeUseParetoDistInRegion bool
	sync.RWMutex
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
	case "pareto":
		fieldLengthGenerator = generator.NewPareto(fieldLength, generator.ParetoTheta, generator.ParetoK, generator.ParetoSigma)
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

// InitThread implements the Workload InitThread interface.
func (c *core) InitThread(ctx context.Context, _ int, _ int) context.Context {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	fieldNames := make([]string, len(c.fieldNames))
	copy(fieldNames, c.fieldNames)
	state := &coreState{
		r:          r,
		fieldNames: fieldNames,
	}
	return context.WithValue(ctx, stateKey, state)
}

// CleanupThread implements the Workload CleanupThread interface.
func (c *core) CleanupThread(_ context.Context) {

}

// Close implements the Workload Close interface.
func (c *core) Close() error {
	// sort.Ints(c.keyRangeFreqs)
	freqSum := int64(0)
	vsizeSum := int64(0)
	for i, freq := range c.keyRangeFreqs {
		freqSum += freq
		vsizeSum += c.keyRangeSumVSize[i]
	}
	freqSum /= 100
	vsizeSum /= 100
	if freqSum == 0 || vsizeSum == 0 {
		return nil
	}
	fmt.Println("avg valueSize:", vsizeSum/freqSum, "\n region load info:")
	for i, freq := range c.keyRangeFreqs {
		if freq > 0 {
			freqRatio := float64(freq) / float64(freqSum)
			valueSizeRatio := float64(c.keyRangeSumVSize[i]) / float64(vsizeSum)
			if freqRatio >= 0.05 || valueSizeRatio >= 0.05 {
				fmt.Println("range ", i, ": freq ratio (%) ", freqRatio, ", valueSize ratio (%)", valueSizeRatio, "; avg valueSize", float64(c.keyRangeSumVSize[i])/float64(freq))
			}
		}
	}
	return nil
}

func (c *core) buildKeyName(keyNum int64) string {
	if !c.orderedInserts {
		keyNum = util.Hash64(keyNum)
	}

	prefix := c.p.GetString(prop.KeyPrefix, prop.KeyPrefixDefault)
	return fmt.Sprintf("%s%0[3]*[2]d", prefix, keyNum, c.zeroPadding)
}

func (c *core) buildSingleValue(state *coreState, key string) map[string][]byte {
	values := make(map[string][]byte, 1)

	r := state.r
	fieldKey := state.fieldNames[c.fieldChooser.Next(r)]

	var buf []byte
	if c.dataIntegrity {
		buf = c.buildDeterministicValue(state, key, fieldKey)
	} else {
		buf = c.buildRandomValue(state)
	}

	values[fieldKey] = buf

	return values
}

func (c *core) buildValues(state *coreState, key string) map[string][]byte {
	values := make(map[string][]byte, c.fieldCount)

	for _, fieldKey := range state.fieldNames {
		var buf []byte
		if c.dataIntegrity {
			buf = c.buildDeterministicValue(state, key, fieldKey)
		} else {
			buf = c.buildRandomValue(state)
		}

		values[fieldKey] = buf
	}
	return values
}

func (c *core) getValueBuffer(size int) []byte {
	buf := c.valuePool.Get().([]byte)
	if cap(buf) >= size {
		return buf[0:size]
	}

	return make([]byte, size)
}

func (c *core) putValues(values map[string][]byte) {
	for _, value := range values {
		c.valuePool.Put(value)
	}
}

func (c *core) buildRandomValue(state *coreState) []byte {
	// TODO: use pool for the buffer
	r := state.r
	size := c.fieldLengthGenerator.Next(r)
	if c.runMixGraphBench || c.variedSize {
		size = state.curValueSize
	}
	buf := c.getValueBuffer(int(size))
	util.RandBytes(r, buf)
	return buf
}

func (c *core) buildDeterministicValue(state *coreState, key string, fieldKey string) []byte {
	// TODO: use pool for the buffer
	r := state.r
	size := c.fieldLengthGenerator.Next(r)
	if c.runMixGraphBench || c.variedSize {
		size = state.curValueSize
	}
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

func (c *core) verifyRow(state *coreState, key string, values map[string][]byte) {
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
func (c *core) DoInsert(ctx context.Context, db ycsb.DB) error {
	state := ctx.Value(stateKey).(*coreState)
	r := state.r
	keyNum := c.keySequence.Next(r)
	dbKey := c.buildKeyName(keyNum)
	values := c.buildValues(state, dbKey)
	defer c.putValues(values)

	numOfRetries := int64(0)

	var err error
	for {
		err = db.Insert(ctx, c.table, dbKey, values)
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
func (c *core) DoBatchInsert(ctx context.Context, batchSize int, db ycsb.DB) error {
	batchDB, ok := db.(ycsb.BatchDB)
	if !ok {
		return fmt.Errorf("the %T does't implement the batchDB interface", db)
	}
	state := ctx.Value(stateKey).(*coreState)
	r := state.r
	var keys []string
	var values []map[string][]byte
	for i := 0; i < batchSize; i++ {
		keyNum := c.keySequence.Next(r)
		dbKey := c.buildKeyName(keyNum)
		keys = append(keys, dbKey)
		values = append(values, c.buildValues(state, dbKey))
	}
	defer func() {
		for _, value := range values {
			c.putValues(value)
		}
	}()

	numOfRetries := int64(0)
	var err error
	for {
		err = batchDB.BatchInsert(ctx, c.table, keys, values)
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

// PowerCdfInversion is the inverse function of power distribution (y=ax^b)
func PowerCdfInversion(u float64, a float64, b float64) int64 {
	ret := math.Pow((u / a), (1 / b))
	return int64(math.Ceil(ret))
}

// ParetoCdfInversion is the inverse function of Pareto distribution
func ParetoCdfInversion(u float64, theta float64, k float64, sigma float64) int64 {
	val := 0.0
	if k == 0.0 {
		val = theta - sigma*math.Log(u)
	} else {
		val = theta + sigma*(math.Pow(u, -1*k)-1)/k
	}
	return int64(math.Ceil(val))
}

func (c *core) InitMixGraphBench() {
	c.runMixGraphBench = c.p.GetBool(prop.MixGraphBench, prop.MixGraphBenchDefault)

	ratio := []float64{
		c.p.GetFloat64(prop.MixGetRatio, prop.MixGetRatioDefault),
		c.p.GetFloat64(prop.MixPutRatio, prop.MixPutRatioDefault),
		c.p.GetFloat64(prop.MixSeekRatio, prop.MixSeekRatioDefault),
	}

	queryChooser := generator.NewDiscrete()
	for i, r := range ratio {
		queryChooser.Add(r, int64(i))
	}
	c.queryChooser = queryChooser

	c.useDefaultRequest = c.p.GetBool("usedefaultrequest", false)

	c.keyRangeNum = c.p.GetInt64(prop.KeyRangeNum, prop.KeyRangeNumDefault)
	keyRangeDistA := c.p.GetFloat64(prop.KeyRangeDistA, prop.KeyRangeDistADefault)
	keyRangeDistB := c.p.GetFloat64(prop.KeyRangeDistB, prop.KeyRangeDistBDefault)
	keyRangeDistC := c.p.GetFloat64(prop.KeyRangeDistC, prop.KeyRangeDistCDefault)
	keyRangeDistD := c.p.GetFloat64(prop.KeyRangeDistD, prop.KeyRangeDistDDefault)
	if keyRangeDistA != 0.0 || keyRangeDistB != 0.0 || keyRangeDistC != 0.0 || keyRangeDistD != 0.0 {
		c.usePrefixModeling = true
		c.genExp = generator.NewTwoTermExpKeys(c.p.GetBool("zipfianrange", false), c.recordCount, c.keyRangeNum, keyRangeDistA, keyRangeDistB, keyRangeDistC, keyRangeDistD)
	}
	rangeNum := c.keyRangeNum + 1
	c.keyRangeFreqs = make([]int64, rangeNum)
	c.keyRangeSumVSize = make([]int64, rangeNum)
	c.keyRangeSize = c.recordCount / c.keyRangeNum

	c.keyDistA = c.p.GetFloat64(prop.KeyDistA, prop.KeyDistADefault)
	c.keyDistB = c.p.GetFloat64(prop.KeyDistB, prop.KeyDistBDefault)
	// if c.keyDistA == 0 || c.keyDistB == 0 {
	// 	c.useDefaultRequest = true
	// }
	fmt.Println("usePrefixModeling ", c.usePrefixModeling, ", useDefaultRequest ", c.useDefaultRequest)
	c.valueTheta = c.p.GetFloat64(prop.ValueTheta, prop.ValueThetaDefault)
	c.valueK = c.p.GetFloat64(prop.ValueK, prop.ValueKDefault)
	c.valueSigma = c.p.GetFloat64(prop.ValueSigma, prop.ValueSigmaDefault)
	c.iterTheta = c.p.GetFloat64(prop.IterTheta, prop.IterThetaDefault)
	c.iterK = c.p.GetFloat64(prop.IterK, prop.IterKDefault)
	c.iterSigma = c.p.GetFloat64(prop.IterSigma, prop.IterSigmaDefault)

	c.variedSize = c.p.GetBool("variedsize", false)
	c.avgValueSizes = []int64{}

	r := rand.New(rand.NewSource(2))
	avg := int64(0)
	smallValPos := []int64{}
	bigValPos := []int64{}
	for i := int64(0); i < c.keyRangeNum+1; i++ {
		u := r.Float64()
		cv := ParetoCdfInversion(u, c.valueTheta, c.valueK, c.valueSigma)
		if cv <= 0 {
			cv = 10
		}

		if cv >= 10 && cv <= 100 {
			smallValPos = append(smallValPos, i)
		}
		if cv >= 1024*3 && cv <= 10240 {
			bigValPos = append(bigValPos, i)
		}

		avg += cv
		c.avgValueSizes = append(c.avgValueSizes, cv)
	}
	avg /= c.keyRangeNum

	exp := c.valueSigma / (1.0 - c.valueK)
	fmt.Printf("avg avg %v, exp %v\n", avg, exp)

	c.nextShuffleRegionTime = time.Now().Add(time.Duration(c.shuffleRegionInterval * uint64(time.Second)))
	c.shuffleRegionInterval = c.p.GetUint64(prop.ShuffleRegionInterval, prop.ShuffleRegionIntervalDefault)
	c.needShuffleRegion = c.p.GetBool(prop.NeedShuffleRegion, prop.NeedShuffleRegionDefault)
	c.kvSizeUseParetoDistInRegion = c.p.GetBool(prop.KvSizeUseParetoDistInRegion, false)
	if !c.needShuffleRegion {
		c.genExp.Adjust(smallValPos, bigValPos)
		c.genExp.PrintKeyRangeInfo(c.avgValueSizes)
	}
	c.traceReplay = c.p.GetBool(prop.TraceReplay, prop.TraceReplayDefault)
}

func (c *core) MixGraphBench(ctx context.Context, db ycsb.DB) *ycsb.KVRequest {
	var initRand, randV, keyRand, keySeed int64

	state := ctx.Value(stateKey).(*coreState)
	r := state.r

	initRand = r.Int63() % c.recordCount
	randV = initRand % c.recordCount
	u := float64(randV) / float64(c.recordCount)

	if c.needShuffleRegion && time.Now().After(c.nextShuffleRegionTime) {
		c.Lock()
		if time.Now().After(c.nextShuffleRegionTime) {
			c.genExp.Shuffle()
			c.nextShuffleRegionTime = time.Now().Add(time.Duration(c.shuffleRegionInterval * uint64(time.Second)))
		}
		c.Unlock()
	}

	if c.useDefaultRequest {
		keyRand = c.nextKeyNum(state)
	} else if c.usePrefixModeling {
		keyRand = c.genExp.DistGetKeyID(false, r, initRand, c.keyDistA, c.keyDistB)
	} else {
		keySeed = PowerCdfInversion(u, c.keyDistA, c.keyDistB)
		keyRand = util.Hash64(keySeed) % c.recordCount
	}
	keyName := c.buildKeyName(keyRand)
	queryType := c.queryChooser.Next(r) //or generated by randV

	curRange := keyRand / c.keyRangeSize
	c.keyRangeFreqs[keyRand/c.keyRangeSize]++

	kvReq := &ycsb.KVRequest{
		QueryType: queryType,
		KeyName:   keyName,
	}

	if queryType == 0 {
		var fields []string
		if !c.readAllFields {
			fieldName := state.fieldNames[c.fieldChooser.Next(r)]
			fields = append(fields, fieldName)
		} else {
			fields = state.fieldNames
		}

		kvReq.Fields = fields
	} else if queryType == 1 {
		// state.curValueSize = ParetoCdfInversion(u, c.valueTheta, c.valueK, c.valueSigma)
		if c.kvSizeUseParetoDistInRegion {
			state.curValueSize = ParetoCdfInversion(u, c.valueTheta, c.valueK, float64(c.avgValueSizes[curRange]))
		} else {
			state.curValueSize = int64(float64(c.avgValueSizes[curRange]) * (0.5*u + 0.75))
		}
		if state.curValueSize <= 0 {
			state.curValueSize = 10
		} else if state.curValueSize > c.fieldLength {
			state.curValueSize = state.curValueSize % c.fieldLength
		}

		c.keyRangeSumVSize[keyRand/c.keyRangeSize] += state.curValueSize

		var values map[string][]byte
		if c.writeAllFields {
			values = c.buildValues(state, keyName)
		} else {
			values = c.buildSingleValue(state, keyName)
		}

		kvReq.Values = values
	} else {
		scanLen := ParetoCdfInversion(u, c.iterTheta, c.iterK, c.iterSigma) % c.maxScanLength

		var fields []string
		if !c.readAllFields {
			fieldName := state.fieldNames[c.fieldChooser.Next(r)]
			fields = append(fields, fieldName)
		} else {
			fields = state.fieldNames
		}

		kvReq.Fields = fields
		kvReq.ScanLen = int(scanLen)
	}

	return kvReq
}

func (c *core) ProcessKVRequest(ctx context.Context, db ycsb.DB, kvReq *ycsb.KVRequest) error {
	switch kvReq.QueryType {
	case 0:
		_, err := db.Read(ctx, c.table, kvReq.KeyName, kvReq.Fields)
		return err
	case 1:
		defer c.putValues(kvReq.Values)
		return db.Insert(ctx, c.table, kvReq.KeyName, kvReq.Values)
	default:
		_, err := db.Scan(ctx, c.table, kvReq.KeyName, kvReq.ScanLen, kvReq.Fields)
		return err
	}
}

// DoTransaction implements the Workload DoTransaction interface.
func (c *core) DoTransaction(ctx context.Context, db ycsb.DB) error {
	if c.runMixGraphBench {
		kvReq := c.MixGraphBench(ctx, db)
		if c.traceReplay {
			client.KVReqGenChan <- kvReq
			return nil
		} else {
			return c.ProcessKVRequest(ctx, db, kvReq)
		}
	}

	state := ctx.Value(stateKey).(*coreState)
	r := state.r

	operation := operationType(c.operationChooser.Next(r))
	switch operation {
	case read:
		return c.doTransactionRead(ctx, db, state)
	case update:
		return c.doTransactionUpdate(ctx, db, state)
	case insert:
		return c.doTransactionInsert(ctx, db, state)
	case scan:
		return c.doTransactionScan(ctx, db, state)
	default:
		return c.doTransactionReadModifyWrite(ctx, db, state)
	}
}

// DoBatchTransaction implements the Workload DoBatchTransaction interface
func (c *core) DoBatchTransaction(ctx context.Context, batchSize int, db ycsb.DB) error {
	batchDB, ok := db.(ycsb.BatchDB)
	if !ok {
		return fmt.Errorf("the %T does't implement the batchDB interface", db)
	}
	state := ctx.Value(stateKey).(*coreState)
	r := state.r

	operation := operationType(c.operationChooser.Next(r))
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

func (c *core) nextKeyNum(state *coreState) int64 {
	r := state.r
	keyNum := int64(0)
	if _, ok := c.keyChooser.(*generator.Exponential); ok {
		keyNum = -1
		for keyNum < 0 {
			keyNum = c.transactionInsertKeySequence.Last() - c.keyChooser.Next(r)
		}
	} else {
		keyNum = math.MaxInt64
		for keyNum > c.transactionInsertKeySequence.Last() {
			keyNum = c.keyChooser.Next(r)
		}
	}
	return keyNum
}

func (c *core) doTransactionRead(ctx context.Context, db ycsb.DB, state *coreState) error {
	r := state.r
	keyNum := c.nextKeyNum(state)
	keyName := c.buildKeyName(keyNum)

	var fields []string
	if !c.readAllFields {
		fieldName := state.fieldNames[c.fieldChooser.Next(r)]
		fields = append(fields, fieldName)
	} else {
		fields = state.fieldNames
	}

	values, err := db.Read(ctx, c.table, keyName, fields)
	if err != nil {
		return err
	}

	if c.dataIntegrity {
		c.verifyRow(state, keyName, values)
	}

	return nil
}

func (c *core) doTransactionReadModifyWrite(ctx context.Context, db ycsb.DB, state *coreState) error {
	start := time.Now()
	defer func() {
		measurement.Measure("READ_MODIFY_WRITE", time.Now().Sub(start))
	}()

	r := state.r
	keyNum := c.nextKeyNum(state)
	keyName := c.buildKeyName(keyNum)

	var fields []string
	if !c.readAllFields {
		fieldName := state.fieldNames[c.fieldChooser.Next(r)]
		fields = append(fields, fieldName)
	} else {
		fields = state.fieldNames
	}

	var values map[string][]byte
	if c.writeAllFields {
		values = c.buildValues(state, keyName)
	} else {
		values = c.buildSingleValue(state, keyName)
	}
	defer c.putValues(values)

	readValues, err := db.Read(ctx, c.table, keyName, fields)
	if err != nil {
		return err
	}

	if err := db.Update(ctx, c.table, keyName, values); err != nil {
		return err
	}

	if c.dataIntegrity {
		c.verifyRow(state, keyName, readValues)
	}

	return nil
}

func (c *core) doTransactionInsert(ctx context.Context, db ycsb.DB, state *coreState) error {
	r := state.r
	keyNum := c.transactionInsertKeySequence.Next(r)
	defer c.transactionInsertKeySequence.Acknowledge(keyNum)
	dbKey := c.buildKeyName(keyNum)
	values := c.buildValues(state, dbKey)
	defer c.putValues(values)

	return db.Insert(ctx, c.table, dbKey, values)
}

func (c *core) doTransactionScan(ctx context.Context, db ycsb.DB, state *coreState) error {
	r := state.r
	keyNum := c.nextKeyNum(state)
	startKeyName := c.buildKeyName(keyNum)

	scanLen := c.scanLength.Next(r)

	var fields []string
	if !c.readAllFields {
		fieldName := state.fieldNames[c.fieldChooser.Next(r)]
		fields = append(fields, fieldName)
	} else {
		fields = state.fieldNames
	}

	_, err := db.Scan(ctx, c.table, startKeyName, int(scanLen), fields)

	return err
}

func (c *core) doTransactionUpdate(ctx context.Context, db ycsb.DB, state *coreState) error {
	keyNum := c.nextKeyNum(state)
	keyName := c.buildKeyName(keyNum)

	var values map[string][]byte
	if c.writeAllFields {
		values = c.buildValues(state, keyName)
	} else {
		values = c.buildSingleValue(state, keyName)
	}

	defer c.putValues(values)

	return db.Update(ctx, c.table, keyName, values)
}

func (c *core) doBatchTransactionRead(ctx context.Context, batchSize int, db ycsb.BatchDB, state *coreState) error {
	r := state.r
	var fields []string

	if !c.readAllFields {
		fieldName := state.fieldNames[c.fieldChooser.Next(r)]
		fields = append(fields, fieldName)
	} else {
		fields = state.fieldNames
	}

	keys := make([]string, batchSize)
	for i := 0; i < batchSize; i++ {
		keys[i] = c.buildKeyName(c.nextKeyNum(state))
	}

	_, err := db.BatchRead(ctx, c.table, keys, fields)
	if err != nil {
		return err
	}

	// TODO should we verify the result?
	return nil
}

func (c *core) doBatchTransactionInsert(ctx context.Context, batchSize int, db ycsb.BatchDB, state *coreState) error {
	r := state.r
	keys := make([]string, batchSize)
	values := make([]map[string][]byte, batchSize)
	for i := 0; i < batchSize; i++ {
		keyNum := c.transactionInsertKeySequence.Next(r)
		keyName := c.buildKeyName(keyNum)
		keys[i] = keyName
		if c.writeAllFields {
			values[i] = c.buildValues(state, keyName)
		} else {
			values[i] = c.buildSingleValue(state, keyName)
		}
		c.transactionInsertKeySequence.Acknowledge(keyNum)
	}

	defer func() {
		for _, value := range values {
			c.putValues(value)
		}
	}()

	return db.BatchInsert(ctx, c.table, keys, values)
}

func (c *core) doBatchTransactionUpdate(ctx context.Context, batchSize int, db ycsb.BatchDB, state *coreState) error {
	keys := make([]string, batchSize)
	values := make([]map[string][]byte, batchSize)
	for i := 0; i < batchSize; i++ {
		keyNum := c.nextKeyNum(state)
		keyName := c.buildKeyName(keyNum)
		keys[i] = keyName
		if c.writeAllFields {
			values[i] = c.buildValues(state, keyName)
		} else {
			values[i] = c.buildSingleValue(state, keyName)
		}
	}

	defer func() {
		for _, value := range values {
			c.putValues(value)
		}
	}()

	return db.BatchUpdate(ctx, c.table, keys, values)
}

// CoreCreator creates the Core workload.
type coreCreator struct {
}

// Create implements the WorkloadCreator Create interface.
func (coreCreator) Create(p *properties.Properties) (ycsb.Workload, error) {
	c := new(core)
	c.p = p
	c.table = p.GetString(prop.TableName, prop.TableNameDefault)
	c.fieldCount = p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
	c.fieldNames = make([]string, c.fieldCount)
	for i := int64(0); i < c.fieldCount; i++ {
		c.fieldNames[i] = fmt.Sprintf("field%d", i)
	}
	c.fieldLengthGenerator = getFieldLengthGenerator(p)
	c.fieldLength = p.GetInt64(prop.FieldLength, prop.FieldLengthDefault)
	c.recordCount = p.GetInt64(prop.RecordCount, prop.RecordCountDefault)
	if c.recordCount == 0 {
		c.recordCount = int64(math.MaxInt32)
	}

	requestDistrib := p.GetString(prop.RequestDistribution, prop.RequestDistributionDefault)
	maxScanLength := p.GetInt64(prop.MaxScanLength, prop.MaxScanLengthDefault)
	scanLengthDistrib := p.GetString(prop.ScanLengthDistribution, prop.ScanLengthDistributionDefault)
	c.maxScanLength = maxScanLength

	insertStart := p.GetInt64(prop.InsertStart, prop.InsertStartDefault)
	insertCount := p.GetInt64(prop.InsertCount, c.recordCount-insertStart)
	if c.recordCount < insertStart+insertCount {
		util.Fatalf("record count %d must be bigger than insert start %d + count %d",
			c.recordCount, insertStart, insertCount)
	}
	c.zeroPadding = p.GetInt64(prop.ZeroPadding, prop.ZeroPaddingDefault)
	c.readAllFields = p.GetBool(prop.ReadAllFields, prop.ReadALlFieldsDefault)
	c.writeAllFields = p.GetBool(prop.WriteAllFields, prop.WriteAllFieldsDefault)
	c.dataIntegrity = p.GetBool(prop.DataIntegrity, prop.DataIntegrityDefault)
	fieldLengthDistribution := p.GetString(prop.FieldLengthDistribution, prop.FieldLengthDistributionDefault)
	if c.dataIntegrity && fieldLengthDistribution != "constant" {
		util.Fatal("must have constant field size to check data integrity")
	}

	if p.GetString(prop.InsertOrder, prop.InsertOrderDefault) == "hashed" {
		c.orderedInserts = false
	} else {
		c.orderedInserts = true
	}

	c.keySequence = generator.NewCounter(insertStart)
	c.operationChooser = createOperationGenerator(p)

	c.transactionInsertKeySequence = generator.NewAcknowledgedCounter(c.recordCount)
	switch requestDistrib {
	case "uniform":
		c.keyChooser = generator.NewUniform(insertStart, insertStart+insertCount-1)
	case "sequential":
		c.keyChooser = generator.NewSequential(insertStart, insertStart+insertCount-1)
	case "zipfian":
		insertProportion := p.GetFloat64(prop.InsertProportion, prop.InsertProportionDefault)
		opCount := p.GetInt64(prop.OperationCount, 0)
		expectedNewKeys := int64(float64(opCount) * insertProportion * 2.0)
		c.keyChooser = generator.NewScrambledZipfian(insertStart, insertStart+insertCount+expectedNewKeys, generator.ZipfianConstant)
	case "latest":
		c.keyChooser = generator.NewSkewedLatest(c.transactionInsertKeySequence)
	case "hotspot":
		hotsetFraction := p.GetFloat64(prop.HotspotDataFraction, prop.HotspotDataFractionDefault)
		hotopnFraction := p.GetFloat64(prop.HotspotOpnFraction, prop.HotspotOpnFractionDefault)
		c.keyChooser = generator.NewHotspot(insertStart, insertStart+insertCount-1, hotsetFraction, hotopnFraction)
	case "exponential":
		percentile := p.GetFloat64(prop.ExponentialPercentile, prop.ExponentialPercentileDefault)
		frac := p.GetFloat64(prop.ExponentialFrac, prop.ExponentialFracDefault)
		c.keyChooser = generator.NewExponential(percentile, float64(c.recordCount)*frac)
	default:
		util.Fatalf("unknown request distribution %s", requestDistrib)
	}

	c.fieldChooser = generator.NewUniform(0, c.fieldCount-1)
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

	c.InitMixGraphBench()

	return c, nil
}

func init() {
	ycsb.RegisterWorkloadCreator("core", coreCreator{})
}
