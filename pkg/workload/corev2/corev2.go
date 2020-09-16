package corev2

import (
	"context"
	"fmt"
	"github.com/magiconair/properties"
	"github.com/pingcap/errors"
	"github.com/pingcap/go-ycsb/pkg/generator"
	"github.com/pingcap/go-ycsb/pkg/measurement"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/pingcap/go-ycsb/pkg/workload"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type contextKey string

const stateKey = contextKey("corev2")

const (
	// maxTimestamp = "2038-01-19T03:14:07.999999Z"
	maxTimeUnixNano = 2147483647999999000
	// minTimestamp = "1970-01-01T00:00:01.000000Z"
	minTimeUnixNano = 1000000000
)

type coreState struct {
	*workload.CoreState
	//r *rand.Rand
	// fields is a copy of corev2.fields to be groutine-local
	fields map[string]string
}

type corev2 struct {
	workload.Core
	fields map[string]string
}

func (c *corev2) InitThread(ctx context.Context, _ int, _ int) context.Context {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var fieldNames []string
	for name, _ := range c.fields {
		fieldNames = append(fieldNames, name)
	}
	state := &coreState{
		fields: c.fields,
		CoreState: &workload.CoreState{
			R: r,
			FieldNames: fieldNames,
		},
	}
	return context.WithValue(ctx, stateKey, state)
}

func (c *corev2) CleanupThread(_ context.Context) {}

func (c *corev2) Close() error { return nil }

func isStringType(fieldType string) bool {
	fieldType = strings.ToLower(fieldType)
	if strings.Contains(fieldType, "char") ||
		strings.Contains(fieldType, "text") ||
		strings.Contains(fieldType, "string") {
		return true
	}
	return false
}

func isBoolType(fieldType string) bool {
	fieldType = strings.ToLower(fieldType)
	if strings.Contains(fieldType, "bool") {
		return true
	}
	return false
}

func isInt64Type(fieldType string) bool {
	fieldType = strings.ToLower(fieldType)
	if fieldType == "bigint" || fieldType == "int64" {
		return true
	}
	return false
}

func isTimestampType(fieldType string) bool {
	fieldType = strings.ToLower(fieldType)
	if fieldType == "timestamp" {
		return true
	}
	return false
}

func (c *corev2) buildRandomString(state *coreState, size int) []byte {
	r := state.R
	buf := c.GetValueBuffer(size)
	util.RandBytes(r, buf)
	return buf
}

func (c *corev2) buildRandomInt64(state *coreState) []byte {
	return []byte(strconv.FormatInt(state.R.Int63(), 10))
}

func (c *corev2) buildRandomBool(state *coreState) []byte {
	choices := []string{"true", "false"}
	return []byte(choices[state.R.Intn(2)])
}

func (c *corev2) buildRandomTimestamp(state *coreState) []byte {
	randomUnixNano := state.R.Int63n(maxTimeUnixNano - minTimeUnixNano) + minTimeUnixNano
	return []byte(time.Unix(0, randomUnixNano).Format("2006-01-02 15:04:05.000000"))
}

func (c *corev2) buildValues(state *coreState) (map[string][]byte, error) {
	values := make(map[string][]byte, c.FieldCount)
	for fieldKey, fieldType := range state.fields {
		if isStringType(fieldType) {
			re := regexp.MustCompile("[0-9]+")
			size := re.Find([]byte(fieldType))
			s, err := strconv.Atoi(string(size))
			if err != nil {
				return nil, err
			}
			values[fieldKey] = c.buildRandomString(state, s)
		} else if isInt64Type(fieldType) {
			values[fieldKey] = c.buildRandomInt64(state)
		} else if isBoolType(fieldType) {
			values[fieldKey] = c.buildRandomBool(state)
		} else if isTimestampType(fieldType) {
			values[fieldKey] = c.buildRandomTimestamp(state)
		} else {
			return nil, errors.New(fmt.Sprintf("Unsupported field type: %s for field key: %s", fieldType, fieldType))
		}
	}
	return values, nil
}

//func (c *corev2) putValues(values map[string][]byte) {
//	for _, value := range values {
//		c.ValuePool.Put(value)
//	}
//}

func (c *corev2) DoInsert(ctx context.Context, db ycsb.DB) error {
	state := ctx.Value(stateKey).(*coreState)
	r := state.R
	keyNum := c.KeySequence.Next(r)
	dbKey := c.BuildKeyName(keyNum)
	values, err := c.buildValues(state)
	if err != nil {
		return err
	}
	strs := make(map[string][]byte, 0)
	for fieldKey, fieldType := range c.fields {
		if isStringType(fieldType) {
			strs[fieldKey] = values[fieldKey]
		}
	}
	defer c.PutValues(strs)

	numOfRetries := int64(0)
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
		numOfRetries++
		if numOfRetries > c.InsertionRetryLimit {
			break
		}
		sleepTimeMs := float64((c.InsertionRetryInterval * 1000)) * (0.8 + 0.4 * r.Float64())
		time.Sleep(time.Duration(sleepTimeMs) * time.Millisecond)
	}
	return err
}

func (c *corev2) DoBatchInsert(ctx context.Context, batchSize int, db ycsb.DB) error {
	batchDB, ok := db.(ycsb.BatchDB)
	if !ok {
		return fmt.Errorf("the %T doesn't implement the batchDB interface", db)
	}
	state := ctx.Value(stateKey).(*coreState)
	r := state.R
	var keys []string
	var values []map[string][]byte

	for i:=0; i<batchSize; i++ {
		keyNum := c.KeySequence.Next(r)
		dbKey := c.BuildKeyName(keyNum)
		keys = append(keys, dbKey)
		genValues, err := c.buildValues(state)
		if err != nil {
			return err
		}
		values = append(values, genValues)
	}
	defer func() {
		for _, genValues := range values {
			c.putStrings(genValues)
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
		numOfRetries++
		if numOfRetries > c.InsertionRetryLimit {
			break
		}
		sleepTimeMs := float64((c.InsertionRetryInterval * 1000)) * (0.8 + 0.4*r.Float64())
		time.Sleep(time.Duration(sleepTimeMs) * time.Millisecond)
	}
	return err
}

func (c *corev2) DoTransaction(ctx context.Context, db ycsb.DB) error {
	state := ctx.Value(stateKey).(*coreState)
	r := state.R

	operation := workload.OperationType(c.OperationChooser.Next(r))
	switch operation {
	case workload.Read:
		return c.DoTransactionRead(ctx, db, state.CoreState)
	case workload.Update:
		return c.DoTransactionUpdate(ctx, db, state)
	case workload.Insert:
		return c.DoTransactionInsert(ctx, db, state)
	case workload.Scan:
		return c.DoTransactionScan(ctx, db, state.CoreState)
	default:
		return c.DoTransactionReadModifyWrite(ctx, db, state)
	}
}

func (c *corev2) DoTransactionUpdate(ctx context.Context, db ycsb.DB, state *coreState) error {
	keyNum := c.NextKeyNum(state.CoreState)
	keyName := c.BuildKeyName(keyNum)
	var values map[string][]byte
	var err error
	if c.WriteAllFields {
		if values, err = c.buildValues(state); err != nil {
			return err
		}
	} else {
		if values, err = c.buildSingleValue(state); err != nil {
			return err
		}
	}
	c.putStrings(values)
	return db.Update(ctx, c.Table, keyName, values)
}

func (c *corev2) putStrings(values map[string][]byte) {
	strs := make(map[string][]byte)
	for fieldKey, fieldType := range c.fields {
		if isStringType(fieldType) {
			strs[fieldKey] = values[fieldKey]
		}
	}
	c.PutValues(strs)
}

func (c *corev2) DoTransactionInsert(ctx context.Context, db ycsb.DB, state *coreState) error {
	keyNum := c.TransactionInsertKeySequence.Next(state.R)
	defer c.TransactionInsertKeySequence.Acknowledge(keyNum)
	keyName := c.BuildKeyName(keyNum)
	values, err := c.buildValues(state)
	if err != nil {
		return err
	}
	defer c.putStrings(values)
	return db.Insert(ctx, c.Table, keyName, values)
}

func (c *corev2) DoTransactionReadModifyWrite(ctx context.Context, db ycsb.DB, state *coreState) error {
	start := time.Now()
	defer measurement.Measure("READ_MODIFY_WRITE", time.Now().Sub(start))
	keyNum := c.NextKeyNum(state.CoreState)
	keyName := c.BuildKeyName(keyNum)
	var fields []string
	var values map[string][]byte
	var err error
	if !c.ReadAllFields {
		fieldName := state.FieldNames[c.FieldChooser.Next(state.R)]
		fields = append(fields, fieldName)
		if values, err = c.buildSingleValue(state); err != nil {
			return err
		}
	} else {
		fields = state.FieldNames
		if values, err = c.buildValues(state); err != nil {
			return err
		}
	}
	defer c.putStrings(values)
	_, err = db.Read(ctx, c.Table, keyName, fields)
	if err != nil {
		return err
	}
	err = db.Update(ctx, c.Table, keyName, values)
	return err
}

func (c *corev2) buildSingleValue(state *coreState) (map[string][]byte, error) {
	values := make(map[string][]byte)
	r := state.R
	fieldName := state.FieldNames[c.FieldChooser.Next(r)]
	fieldType := c.fields[fieldName]

	buf, err := c.buildRandomValue(state, fieldType)
	if err != nil {
		return nil, err
	}
	values[fieldName] = buf
	return values, nil
}

func (c *corev2) buildRandomValue(state *coreState, fieldType string) ([]byte, error) {
	if isStringType(fieldType) {
		re := regexp.MustCompile("[0-9]+")
		size := re.Find([]byte(fieldType))
		s, err := strconv.Atoi(string(size))
		if err != nil {
			return nil, err
		}
		return c.buildRandomString(state, s), nil
	} else if isInt64Type(fieldType) {
		return c.buildRandomInt64(state), nil
	} else if isBoolType(fieldType) {
		return c.buildRandomBool(state), nil
	} else if isTimestampType(fieldType) {
		return c.buildRandomTimestamp(state), nil
	} else {
		return nil, errors.New(fmt.Sprintf("Unsupported field type: %s for field key: %s", fieldType, fieldType))
	}
}

func (c *corev2) DoBatchTransaction(ctx context.Context, batchSize int, db ycsb.DB) error {
	batchDB, ok := db.(ycsb.BatchDB)
	if !ok {
		return fmt.Errorf("the %T doesn't implement the batchDB interface", db)
	}
	state := ctx.Value(stateKey).(*coreState)
	operation := workload.OperationType(c.OperationChooser.Next(state.R))
	switch operation {
	case workload.Read:
		return c.DoBatchTransactionRead(ctx, batchSize, batchDB, state.CoreState)
	case workload.Insert:
		return c.DoBatchTransactionInsert(ctx, batchSize, batchDB, state)
	case workload.Update:
		return c.DoBatchTransactionUpdate(ctx, batchSize, batchDB, state)
	case workload.Scan:
		panic("The batch mode doesn't support the Scan operation")
	default:
		return nil
	}
}

func (c *corev2) DoBatchTransactionInsert(ctx context.Context, batchSize int, db ycsb.BatchDB, state *coreState) error {
	keys := make([]string, batchSize)
	values := make([]map[string][]byte, batchSize)
	var err error
	for i := 0; i < batchSize; i++ {
		keyNum := c.TransactionInsertKeySequence.Next(state.R)
		keyName := c.BuildKeyName(keyNum)
		keys[i] = keyName
		if c.WriteAllFields {
			if values[i], err = c.buildValues(state); err != nil {
				return err
			}
		} else {
			if values[i], err = c.buildSingleValue(state); err != nil {
				return err
			}
		}
		c.TransactionInsertKeySequence.Acknowledge(keyNum)
	}
	defer func() {
		for _, value := range values {
			c.putStrings(value)
		}
	}()
	return db.BatchInsert(ctx, c.Table, keys, values)
}

func (c *corev2) DoBatchTransactionUpdate(ctx context.Context, batchSize int, db ycsb.BatchDB, state *coreState) error {
	keys := make([]string, batchSize)
	values := make([]map[string][]byte, batchSize)
	var err error
	for i := 0; i < batchSize; i++ {
		keyNum := c.NextKeyNum(state.CoreState)
		keyName := c.BuildKeyName(keyNum)
		keys[i] = keyName
		if c.WriteAllFields {
			if values[i], err = c.buildValues(state); err != nil {
				return err
			}
		} else {
			if values[i], err = c.buildSingleValue(state); err != nil {
				return err
			}
		}
	}

	defer func() {
		for _, value := range values {
			c.putStrings(value)
		}
	}()

	return db.BatchUpdate(ctx, c.Table, keys, values)
}

type corev2Creator struct {}

func (corev2Creator) Create(p *properties.Properties) (ycsb.Workload, error) {
	c := new(corev2)
	c.P = p
	c.Table = p.GetString(prop.TableName, prop.TableNameDefault)
	c.fields = make(map[string]string)
	fieldsDef := strings.Split(p.GetString(prop.Fields, prop.FieldsDefault), ",")
	c.FieldCount = int64(len(fieldsDef))
	for _, fieldDef := range fieldsDef {
		def := strings.Split(fieldDef, " ")
		if len(def) != 2 {
			return nil, errors.New(fmt.Sprintf("Field definition must include name and type. Got: %s", fieldDef))
		}
		fieldName := def[0]
		fieldType := def[1]
		c.fields[fieldName] = fieldType
	}
	c.RecordCount = p.GetInt64(prop.RecordCount, prop.FieldCountDefault)
	requestDistrib := p.GetString(prop.RequestDistribution, prop.RequestDistributionDefault)
	maxScanLength := p.GetInt64(prop.MaxScanLength, prop.MaxScanLengthDefault)
	scanLengthDistrib := p.GetString(prop.ScanLengthDistribution, prop.ScanLengthDistributionDefault)

	insertStart := p.GetInt64(prop.InsertStart, prop.InsertStartDefault)
	insertCount := p.GetInt64(prop.InsertCount, c.RecordCount-insertStart)
	if c.RecordCount < insertStart+insertCount {
		util.Fatalf("record count %d must be bigger than insert start %d + count %d",
			c.RecordCount, insertStart, insertCount)
	}
	c.ZeroPadding = p.GetInt64(prop.ZeroPadding, prop.ZeroPaddingDefault)
	c.ReadAllFields = p.GetBool(prop.ReadAllFields, prop.ReadALlFieldsDefault)
	c.WriteAllFields = p.GetBool(prop.WriteAllFields, prop.WriteAllFieldsDefault)
	c.DataIntegrity = p.GetBool(prop.DataIntegrity, prop.DataIntegrityDefault)
	fieldLengthDistribution := p.GetString(prop.FieldLengthDistribution, prop.FieldLengthDistributionDefault)
	if c.DataIntegrity && fieldLengthDistribution != "constant" {
		util.Fatal("must have constant field size to check data integrity")
	}
	if p.GetString(prop.InsertOrder, prop.InsertOrderDefault) == "hashed" {
		c.OrderedInserts = false
	} else {
		c.OrderedInserts = true
	}

	c.KeySequence = generator.NewCounter(insertStart)
	c.OperationChooser = workload.CreateOperationGenerator(p)

	c.TransactionInsertKeySequence = generator.NewAcknowledgedCounter(c.RecordCount)
	switch requestDistrib {
	case "uniform":
		c.KeyChooser = generator.NewUniform(insertStart, insertStart+insertCount-1)
	case "sequential":
		c.KeyChooser = generator.NewSequential(insertStart, insertStart+insertCount-1)
	case "zipfian":
		insertProportion := p.GetFloat64(prop.InsertProportion, prop.InsertProportionDefault)
		opCount := p.GetInt64(prop.OperationCount, 0)
		expectedNewKeys := int64(float64(opCount) * insertProportion * 2.0)
		c.KeyChooser = generator.NewScrambledZipfian(insertStart, insertStart+insertCount+expectedNewKeys, generator.ZipfianConstant)
	case "latest":
		c.KeyChooser = generator.NewSkewedLatest(c.TransactionInsertKeySequence)
	case "hotspot":
		hotsetFraction := p.GetFloat64(prop.HotspotDataFraction, prop.HotspotDataFractionDefault)
		hotopnFraction := p.GetFloat64(prop.HotspotOpnFraction, prop.HotspotOpnFractionDefault)
		c.KeyChooser = generator.NewHotspot(insertStart, insertStart+insertCount-1, hotsetFraction, hotopnFraction)
	case "exponential":
		percentile := p.GetFloat64(prop.ExponentialPercentile, prop.ExponentialPercentileDefault)
		frac := p.GetFloat64(prop.ExponentialFrac, prop.ExponentialFracDefault)
		c.KeyChooser = generator.NewExponential(percentile, float64(c.RecordCount)*frac)
	default:
		util.Fatalf("unknown request distribution %s", requestDistrib)
	}

	c.FieldChooser = generator.NewUniform(0, c.FieldCount-1)
	switch scanLengthDistrib {
	case "uniform":
		c.ScanLength = generator.NewUniform(1, maxScanLength)
	case "zipfian":
		c.ScanLength = generator.NewZipfianWithRange(1, maxScanLength, generator.ZipfianConstant)
	default:
		util.Fatalf("distribution %s not allowed for scan length", scanLengthDistrib)
	}

	c.InsertionRetryLimit = p.GetInt64(prop.InsertionRetryLimit, prop.InsertionRetryLimitDefault)
	c.InsertionRetryInterval = p.GetInt64(prop.InsertionRetryInterval, prop.InsertionRetryIntervalDefault)

	fieldLength := p.GetInt64(prop.FieldLength, prop.FieldLengthDefault)
	c.ValuePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, fieldLength)
		},
	}

	return c, nil
}

func init() {
	ycsb.RegisterWorkloadCreator("corev2", corev2Creator{})
}
