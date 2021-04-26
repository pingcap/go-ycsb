package workload

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/measurement"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const SysbenchCmd_Prepare = "prepare"
const SysbenchCmd_Run = "run"
const SysbenchCmd_Cleanup = "cleanup"

// SysbenchType
const TypePointSelect = "oltp_point_select"
const TypeUpdateIndex = "oltp_update_index"
const TypeUpdateNonIndex = "oltp_update_non_index"
const TypeReadWrite = "oltp_read_write"

const QueryPointSelect = string("point_selects")
const QueryIndexUpdate = string("index_updates")
const QueryNonIndexUpdate = string("non_index_updates")
const QuerySimpleRanges = string("simple_ranges")
const QuerySumRanges = string("sum_ranges")
const QueryOrderRagnes = string("order_ranges")
const QueryDistinctRanges = string("distinct_ranges")
const QueryDeletes = string("deletes")
const QueryInserts = string("inserts")

var statDefs = map[string]string{
	QueryPointSelect:    "SELECT c FROM sbtest%v WHERE id=?",
	QueryIndexUpdate:    "UPDATE sbtest%v SET k=k+1 WHERE id=?",
	QueryNonIndexUpdate: "UPDATE sbtest%v SET c=? WHERE id=?",
	QuerySimpleRanges:   "SELECT c FROM sbtest%v WHERE id BETWEEN ? AND ?",
	QuerySumRanges:      "SELECT SUM(k) FROM sbtest%v WHERE id BETWEEN ? AND ?",
	QueryOrderRagnes:    "SELECT c FROM sbtest%v WHERE id BETWEEN ? AND ? ORDER BY c",
	QueryDistinctRanges: "SELECT DISTINCT c FROM sbtest%v WHERE id BETWEEN ? AND ? ORDER BY c",
	QueryDeletes:        "DELETE FROM sbtest%v WHERE id=?",
	QueryInserts:        "INSERT INTO sbtest%v (id, k, c, pad) VALUES (?, ?, ?, ?)",
}

type sysBench struct {
	p                 *properties.Properties
	db                ycsb.DB
	tableCnt          int   //table count
	tableSize         int64 //table row count
	threadCnt         int
	eventCnt          int64 //test count
	indexUpdateCnt    int
	nonIndexUpdateCnt int
	cValueLen         int
	padLen            int
	rangeSize         int
	simpleRangesCnt   int
	sumRangesCnt      int
	orderRangesCnt    int
	distinctRangsCnt  int
	deleteInsertCnt   int
	hasRangeSelect    int
	skipTrx           int

	// key:workloadtype, value: querylist
	operations map[string][]string
	// workLoadType:handler
	events map[string](func(ctx context.Context, w *sysbenchWorker) error)
	// queryType: handler
	handlers map[string](func(ctx context.Context, w *sysbenchWorker) error)
}

// All the interface including params need to be re-design later.
// Sysbench workload has nothing todo here
func (s *sysBench) Init(db ycsb.DB) error {
	e := make(map[string]func(ctx context.Context, w *sysbenchWorker) error)
	e[TypePointSelect] = s.eventPointSelect
	e[TypeUpdateIndex] = s.eventIndexUpdate
	e[TypeUpdateNonIndex] = s.eventNonIndexUpdate
	e[TypeReadWrite] = s.eventReadWrite
	s.events = e

	h := make(map[string]func(ctx context.Context, w *sysbenchWorker) error)
	h[QueryPointSelect] = s.execPointSelect
	h[QueryIndexUpdate] = s.execIndexUpdate
	h[QueryNonIndexUpdate] = s.execNonIndexUpdate
	h[QuerySimpleRanges] = s.execSimpleRanges
	h[QuerySumRanges] = s.execSumRanges
	h[QuerySimpleRanges] = s.execSimpleRanges
	h[QuerySumRanges] = s.execSumRanges
	h[QueryOrderRagnes] = s.execOrderRanges
	h[QueryDistinctRanges] = s.execDistinctRanges
	// h[QueryDeletes] = s.execDeletes
	// h[QueryInserts] = s.execInserts
	s.handlers = h

	op := make(map[string][]string)
	op[TypePointSelect] = make([]string, 0)
	op[TypePointSelect] = append(op[TypePointSelect], QueryPointSelect)
	op[TypeUpdateIndex] = make([]string, 0)
	op[TypeUpdateIndex] = append(op[TypeUpdateIndex], QueryIndexUpdate)
	op[TypeUpdateNonIndex] = make([]string, 0)
	op[TypeUpdateNonIndex] = append(op[TypeUpdateNonIndex], QueryNonIndexUpdate)
	op[TypeReadWrite] = make([]string, 0)
	op[TypeReadWrite] = append(op[TypeReadWrite], QueryPointSelect,
		QuerySimpleRanges, QuerySumRanges, QueryOrderRagnes, QueryDistinctRanges,
		QueryIndexUpdate, QueryNonIndexUpdate,
		QueryDeletes, QueryInserts,
	)
	s.operations = op

	return nil
}

//TODO return none will be ok later.
func (s *sysBench) Exec(ctx context.Context, tid int) {
	cmdType := s.p.GetString(prop.SysbenchCmdType, "nil")
	wlType := s.p.GetString(prop.SysbenchWorkLoadType, "nil")

	switch cmdType {
	case "prepare":
		s.Prepare(tid)
	case "run":
		s.RunEvent(ctx, tid, wlType)
	case "cleanup":
		s.Cleanup(tid)
	}
}

type sysbenchWorker struct {
	r    *rand.Rand
	conn *sql.Conn
	tid  int
	//map[Queryxxx]:[nil,table1_stmt,table2_stmt]
	stmts map[string][]*sql.Stmt
}

func (s *sysBench) createWorker(ctx context.Context, tid int, wlType string) *sysbenchWorker {
	conn, err := s.db.ToSqlDB().Conn(ctx)
	if err != nil {
		panic(err)
	}
	w := new(sysbenchWorker)
	w.r = rand.New(rand.NewSource(time.Now().UnixNano() + int64(tid)*int64(100000000)))
	w.conn = conn
	w.tid = tid
	w.stmts = s.prepareStatements(ctx, wlType, conn)

	return w
}

func (s *sysBench) releaseWorker(ctx context.Context, w *sysbenchWorker) {
	w.conn.Close()
}

func (s *sysBench) RunEvent(ctx context.Context, tid int, wlType string) {
	var err error
	w := s.createWorker(ctx, tid, wlType)
	defer s.releaseWorker(ctx, w)
	event, ok := s.events[wlType]

	if !ok {
		fmt.Println("sysbench workload type doesn't not exist")
		return
	}

	for i := int64(0); i < s.eventCnt; i++ {
		start := time.Now()
		err = event(ctx, w)

		if err != nil {
			//fmt.Println(err)
			return
		}

		sysBenchMeasure(start, wlType, err)
	}
}

func (s *sysBench) execPointSelect(ctx context.Context, w *sysbenchWorker) error {
	tableId := w.r.Intn(int(s.tableCnt)) + 1
	id := w.r.Int63n(s.tableSize)
	_, err := w.stmts[QueryPointSelect][tableId].ExecContext(ctx, id)

	return err
	//fmt.Println("sysbench point select running with tableID and id", tableId, id)
}
func (s *sysBench) eventPointSelect(ctx context.Context, w *sysbenchWorker) error {
	return s.handlers[QueryPointSelect](ctx, w)
}

func (s *sysBench) execIndexUpdate(ctx context.Context, w *sysbenchWorker) error {
	tableId := w.r.Intn(int(s.tableCnt)) + 1
	id := w.r.Int63n(s.tableSize)
	_, err := w.stmts[QueryIndexUpdate][tableId].ExecContext(ctx, id)

	return err
}

func (s *sysBench) eventIndexUpdate(ctx context.Context, w *sysbenchWorker) error {
	return s.handlers[QueryIndexUpdate](ctx, w)
}

func (s *sysBench) execRanges(ctx context.Context, w *sysbenchWorker, rangeType string) error {
	if s.hasRangeSelect == 0 {
		return nil
	}
	tableId := w.r.Intn(int(s.tableCnt)) + 1

	for i := 1; i <= s.simpleRangesCnt; i++ {
		id := w.r.Int63n(s.tableSize)
		_, err := w.stmts[rangeType][tableId].ExecContext(ctx, id, id+int64(s.rangeSize)-1)
		if err != nil {
			// fmt.Println("execRanges error", err)
			return err
		}
	}
	return nil
}

func (s *sysBench) execNonIndexUpdate(ctx context.Context, w *sysbenchWorker) error {
	tableId := w.r.Intn(int(s.tableCnt)) + 1
	id := w.r.Int63n(s.tableSize)
	c_value := randStringRunes(s.cValueLen, w.r)
	_, err := w.stmts[QueryNonIndexUpdate][tableId].ExecContext(ctx, c_value, id)

	return err
	//fmt.Println("sysbench non index update running with tableID and id", tableId, id)
}

func (s *sysBench) eventNonIndexUpdate(ctx context.Context, w *sysbenchWorker) error {
	return s.handlers[QueryNonIndexUpdate](ctx, w)
}

func (s *sysBench) execSimpleRanges(ctx context.Context, w *sysbenchWorker) error {
	return s.execRanges(ctx, w, QuerySimpleRanges)
}

func (s *sysBench) execSumRanges(ctx context.Context, w *sysbenchWorker) error {
	return s.execRanges(ctx, w, QuerySumRanges)
}

func (s *sysBench) execOrderRanges(ctx context.Context, w *sysbenchWorker) error {
	return s.execRanges(ctx, w, QueryOrderRagnes)
}

func (s *sysBench) execDistinctRanges(ctx context.Context, w *sysbenchWorker) error {
	return s.execRanges(ctx, w, QueryDistinctRanges)
}

func (s *sysBench) execDeleteInserts(ctx context.Context, w *sysbenchWorker) error {
	tableId := w.r.Intn(int(s.tableCnt)) + 1

	for i := 1; i <= s.deleteInsertCnt; i++ {
		id := w.r.Int63n(s.tableSize)
		k := w.r.Int63n(s.tableSize)
		_, err := w.stmts[QueryDeletes][tableId].ExecContext(ctx, id)
		//fmt.Println("thread -", w.tid, "sysbench delete test", statDefs[QueryDeletes], id)
		if err != nil {
			//fmt.Println("sysbench deletes test error", err)
			return err
		}
		c_value := randStringRunes(s.cValueLen, w.r)
		pad := randStringRunes(s.padLen, w.r)
		_, err = w.stmts[QueryInserts][tableId].ExecContext(ctx, id, k, c_value, pad)
		//fmt.Println("thread -", w.tid, "sysbench insert test", statDefs[QueryInserts], id)
		if err != nil {
			//fmt.Println("sysbench inserts test error", err)
			return err
		}
	}
	return nil
}

func (s *sysBench) eventReadWrite(ctx context.Context, w *sysbenchWorker) error {
	//fmt.Println("sysbench read write running...")
	if s.skipTrx == 0 {
		_, err := w.conn.ExecContext(ctx, "begin")
		if err != nil {
			return err
		}
	}

	ops := s.operations[TypeReadWrite]
	for i := 0; i < len(ops); i++ {
		if ops[i] == QueryDeletes || ops[i] == QueryInserts {
			continue
		} else {
			err := s.handlers[ops[i]](ctx, w)
			if err != nil {
				return err
			}
		}
	}

	s.execDeleteInserts(ctx, w)

	if s.skipTrx == 0 {
		_, err := w.conn.ExecContext(ctx, "commit")
		if err != nil {
			return err
		}
	}

	return nil
}

// get the each prepared stmtment of each table for each operation of wlType workload
func (s *sysBench) prepareStatements(ctx context.Context, wlType string, conn *sql.Conn) map[string][]*sql.Stmt {
	//fmt.Println("prepareStatements running...")
	var err error
	cnt := s.tableCnt + 1
	stmts := make(map[string][]*sql.Stmt)
	ops := s.operations[wlType]

	for i := 0; i < len(ops); i++ {
		tablesStmt := make([]*sql.Stmt, cnt)
		query := statDefs[ops[i]]

		for tableID := 1; tableID <= s.tableCnt; tableID++ {
			sql := fmt.Sprintf(query, tableID)
			//fmt.Println("prepareStatements prepare:", query, ops[i])
			tablesStmt[tableID], err = conn.PrepareContext(ctx, sql)
			if err != nil {
				time.Sleep(time.Duration(1) * time.Second)
				panic(err)
			}
		}
		stmts[ops[i]] = tablesStmt
	}

	return stmts
}

func (s *sysBench) Prepare(tid int) {
	s.prepareSysbenchData(tid)
}

func (s *sysBench) Cleanup(tid int) {
	for i := (tid % s.threadCnt) + 1; i <= s.tableCnt; i = i + s.threadCnt {
		query := fmt.Sprintf("drop table if exists sbtest%v", i)
		_, err := s.db.ToSqlDB().Exec(query)
		if err != nil {
			fmt.Println("[Failed]:", query, err)
			return
		}
	}
	fmt.Println("sysbench cleanup database over")
}

func (s *sysBench) Close() error {
	return nil
}

func (s *sysBench) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	return ctx
}

func (s *sysBench) CleanupThread(ctx context.Context) {

}

func (s *sysBench) Load(ctx context.Context, db ycsb.DB, totalCount int64) error {
	return nil
}

func (s *sysBench) DoInsert(ctx context.Context, db ycsb.DB) error {
	return nil
}

func (s *sysBench) DoBatchInsert(ctx context.Context, batchSize int, db ycsb.DB) error {
	return nil
}

func (s *sysBench) DoTransaction(ctx context.Context, db ycsb.DB) error {
	return nil
}

func (s *sysBench) DoBatchTransaction(ctx context.Context, batchSize int, db ycsb.DB) error {
	return nil
}

func (s *sysBench) createSysbenchTable(tableId int) {
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(tableId)*int64(100000000)))
	id_def := string("INTEGER NOT NULL AUTO_INCREMENT")
	id_index_def := string("PRIMARY KEY")
	sql := fmt.Sprintf("CREATE TABLE sbtest%v ("+
		"id %v,"+
		"k INTEGER DEFAULT '0' NOT NULL,"+
		"c CHAR(120) DEFAULT '' NOT NULL,"+
		"pad CHAR(60) DEFAULT '' NOT NULL,"+
		"%v (id)"+
		")",
		tableId, id_def, id_index_def)
	sqlDB := s.db.ToSqlDB()
	dropSql := fmt.Sprintf("drop table if exists sbtest%v", tableId)
	sqlDB.Exec(dropSql)
	_, err := sqlDB.Exec(sql)
	if err != nil {
		panic(err)
	}

	tableSize := s.tableSize
	if tableSize > 0 {
		fmt.Printf("Inserting %v records into sbtest%v\n", tableSize, tableId)
	}
	//TODO deal with auto inc param later
	query := fmt.Sprintf("INSERT INTO sbtest%v (k, c, pad) VALUES", tableId)
	c_value_len := prop.SysbenchCharLength
	pad_value_len := prop.SysbenchPadLength
	bulk_size := prop.SysbenchBulkInsertCount

	bi := new(BulkWorker)
	bi.BulkInsertInit(query, bulk_size, sqlDB)
	var c_value string
	var pad_value string
	var k int64
	for i := int64(0); i < tableSize; i++ {
		k = r.Int63n(tableSize)
		c_value = randStringRunes(c_value_len, r)
		pad_value = randStringRunes(pad_value_len, r)
		query = fmt.Sprintf("(%v,'%v','%v')", k, c_value, pad_value)
		bi.BulkInsertNext(query)
	}
	bi.BulkInsertDone()
	if prop.SysbenchCreateSecondaryIndex != 0 {
		fmt.Printf("Creating a secondary index on 'sbtest%v' ...\n", tableId)
		query = fmt.Sprintf("CREATE INDEX k_%d ON sbtest%d(k)", tableId, tableId)
		sqlDB.Exec(query)
	}
}

//TODO currently assumed mysql, fix it later
func (s *sysBench) prepareSysbenchData(tid int) {
	for i := (tid % s.threadCnt) + 1; i <= s.tableCnt; i = i + s.threadCnt {
		s.createSysbenchTable(i)
	}
}

type sysBenchCreator struct{}

func (sysBenchCreator) Create(p *properties.Properties, db ycsb.DB) (ycsb.Workload, error) {
	s := new(sysBench)
	s.p = p
	s.db = db
	s.tableCnt = p.GetInt(prop.SysbenchTables, prop.SysbenchTablesDefault)
	s.tableSize = p.GetInt64(prop.SysbenchTableSize, prop.SysbenchTableSizeDefault)
	s.threadCnt = p.GetInt(prop.SysbenchThreads, prop.SysbenchThreadsDefault)
	s.eventCnt = p.GetInt64(prop.SysbenchEvents, prop.SysbenchEventsDefault)
	s.indexUpdateCnt = p.GetInt(prop.SysbenchIndexUpdateCnt, prop.SysbenchIndexUpdateCntDefault)
	s.nonIndexUpdateCnt = p.GetInt(prop.SysbenchNonIndexUpdateCnt, prop.SysbenchNonIndexUpdateCntDefault)
	s.cValueLen = prop.SysbenchCharLength
	s.padLen = prop.SysbenchPadLength
	s.rangeSize = p.GetInt(prop.SysbenchRangeSize, prop.SysbenchRangeSizeDefault)
	s.simpleRangesCnt = p.GetInt(prop.SysbenchSimpleRangesCnt, prop.SysbenchSimpleRangesCntDefault)
	s.sumRangesCnt = p.GetInt(prop.SysbenchSumRangesCnt, prop.SysbenchSumRangesCntDefault)
	s.orderRangesCnt = p.GetInt(prop.SysbenchOrderRangesCnt, prop.SysbenchOrderRangesCntDefault)
	s.distinctRangsCnt = p.GetInt(prop.SysbenchDistinctRangeCnt, prop.SysbenchDistinctRangeCntDefault)
	s.deleteInsertCnt = p.GetInt(prop.SysbenchDeleteInsertCnt, prop.SysbenchDeleteInsertCntDefault)
	s.hasRangeSelect = p.GetInt(prop.SysbenchTestRangeSelect, prop.SysbenchTestRangeSelectDefault)
	s.skipTrx = p.GetInt(prop.SysbenchSkipTrx, prop.SysbenchSkipTrxDefault)
	return s, nil
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randStringRunes(n int, r *rand.Rand) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[r.Intn(len(letterRunes))]
	}
	return string(b)
}

type BulkWorker struct {
	bulkSize int
	curSize  int
	db       *sql.DB
	sql      string
	head     string
}

func (w *BulkWorker) BulkInsertInit(head string, bulkSize int, db *sql.DB) {
	w.curSize = 0
	w.sql = head
	w.head = head
	w.db = db
}

//insert into sbtest1(k, c, pad) values
//(),(),();
func (w *BulkWorker) BulkInsertNext(query string) {
	if w.curSize == 0 {
		w.sql += query
	} else {
		w.sql += "," + query
	}
	w.curSize += 1
	if w.curSize >= w.bulkSize {
		w.sql += ";"
		w.BulkInsertDone()
		w.curSize = 0
		w.sql = w.head
	}
}

func (w *BulkWorker) BulkInsertDone() {
	if w.curSize != 0 {
		_, err := w.db.Exec(w.sql)
		if err != nil {
			fmt.Println("exec sql error", w.sql, err)
			panic(err)
		}
	}
}

func sysBenchMeasure(start time.Time, op string, err error) {
	lan := time.Since(start)
	measurement.Measure(op, lan)
}

func init() {
	ycsb.RegisterWorkloadCreator("sysbench", sysBenchCreator{})
}

/***************************************************************************************/
// Sysbench Client
type SysbenchClient struct {
	p        *properties.Properties
	workload ycsb.Workload
	db       ycsb.DB
}

func NewSysbenchClient(p *properties.Properties, workload ycsb.Workload, db ycsb.DB) *SysbenchClient {
	return &SysbenchClient{p: p, workload: workload, db: db}
}

/*
 * exec the workload, and measure report
 */
func (c *SysbenchClient) Run(ctx context.Context) {
	var wg sync.WaitGroup
	threads := c.p.GetInt(prop.SysbenchThreads, prop.SysbenchThreadsDefault)

	wg.Add(threads)
	measureCtx, measureCancel := context.WithCancel(ctx)
	measureCh := make(chan struct{}, 1)
	go func() {
		defer func() {
			measureCh <- struct{}{}
		}()
		dur := c.p.GetInt64(prop.LogInterval, 10)
		t := time.NewTicker(time.Duration(dur) * time.Second)
		defer t.Stop()
		measurement.EnableWarmUp(false)

		for {
			select {
			case <-t.C:
				measurement.Output()
			case <-measureCtx.Done():
				return
			}
		}
	}()

	c.workload.Init(nil)
	for i := 0; i < threads; i++ {
		go func(threadId int) {
			defer wg.Done()
			ctx := c.workload.InitThread(ctx, threadId, threads)
			ctx = c.db.InitThread(ctx, threadId, threads)
			c.workload.Exec(ctx, threadId)
			c.db.CleanupThread(ctx)
			c.workload.CleanupThread(ctx)
		}(i + 1)
	}
	//c.workload.Exec(ctx)
	wg.Wait()
	measureCancel()
	<-measureCh
}
