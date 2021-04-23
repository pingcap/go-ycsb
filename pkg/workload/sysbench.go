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
const SysbenchType_oltp_point_select = "oltp_point_select"
const SysbenchType_oltp_update_index = "oltp_update_index"
const SysbenchType_oltp_update_non_index = "oltp_update_non_index"
const SysbenchType_oltp_read_write = "oltp_read_write"

type sysBench struct {
	p         *properties.Properties
	db        ycsb.DB
	tableCnt  int   //table count
	tableSize int64 //table row count
	threadCnt int
	eventCnt  int64 //test count
}

// All the interface including params need to be re-design later.
// Sysbench workload has nothing todo here
func (s *sysBench) Init(db ycsb.DB) error {
	fmt.Println("sysBench Init running...")
	return nil
}

//TODO return none will be ok later.
func (s *sysBench) Exec(ctx context.Context, tid int) error {
	cmdType := s.p.GetString(prop.SysbenchCmdType, "nil")
	workloadType := s.p.GetString(prop.SysbenchWorkLoadType, "nil")
	creator := GetSysbenchWorkloadCreator(workloadType)
	if creator == nil {
		fmt.Println("sysbench workloadtype doesn't exist, please check your command")
		return nil
	}
	sysBenchWL := creator.Create(s)

	switch cmdType {
	case "prepare":
		sysBenchWL.Prepare(tid)
	case "run":
		sysBenchWL.Run(ctx, tid)
	case "cleanup":
		sysBenchWL.Cleanup(tid)
	}
	return nil
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

type sysBenchCreator struct{}

func (sysBenchCreator) Create(p *properties.Properties, db ycsb.DB) (ycsb.Workload, error) {
	s := new(sysBench)
	s.p = p
	s.db = db
	s.tableCnt = p.GetInt(prop.SysbenchTables, prop.SysbenchTablesDefault)
	s.tableSize = p.GetInt64(prop.SysbenchTableSize, prop.SysbenchTableSizeDefault)
	s.threadCnt = p.GetInt(prop.SysbenchThreads, prop.SysbenchThreadsDefault)
	s.eventCnt = p.GetInt64(prop.SysbenchEvents, prop.SysbenchEventsDefault)
	fmt.Println("sysbench params:", "tables=", s.tableCnt, "table-size=", s.tableSize, "threadCnt=", s.threadCnt, "events=", s.eventCnt)
	return s, nil
}

type SysbenchPointSelectCreator struct{}
type sysbenchWorkloadCreator interface {
	Create(s *sysBench) SysbenchWorkload
}

var sysbenchWorkloadCreators = map[string]sysbenchWorkloadCreator{}

// RegisterWorkloadCreator registers a creator for the workload
func RegisterSysbenchWorkloadCreator(name string, creator sysbenchWorkloadCreator) {
	_, ok := sysbenchWorkloadCreators[name]
	if ok {
		panic(fmt.Sprintf("duplicate register sysbenchWorkloadCreator %s", name))
	}

	sysbenchWorkloadCreators[name] = creator
}

// GetWorkloadCreator gets the WorkloadCreator for the database
func GetSysbenchWorkloadCreator(name string) sysbenchWorkloadCreator {
	return sysbenchWorkloadCreators[name]
}

func (creator SysbenchPointSelectCreator) Create(s *sysBench) SysbenchWorkload {
	fmt.Println("Sysbench SysbenchPointSelect workload creating...")
	w := new(SysbenchPointSelect)
	w.s = s
	return w
}

type SysbenchWorkload interface {
	ID() string
	// prepare the base data for the workload test
	Prepare(tid int)
	// run the workload test
	Run(ctx context.Context, tid int)
	// clean the base data
	Cleanup(tid int)

	GetSysBench() *sysBench
}
type SysbenchPointSelect struct {
	s *sysBench
}

func (ps *SysbenchPointSelect) ID() string {
	return "SysbenchPointSelect"
}
func (ps *SysbenchPointSelect) Prepare(tid int) {
	fmt.Println("SysbenchPointSelect Prepare running ...")
	prepareSysbenchData(ps, tid)
}

func (ps *SysbenchPointSelect) Run(ctx context.Context, tid int) {
	fmt.Println("SysbenchPointSelect Run running...")
	conn, err := ps.s.db.ToSqlDB().Conn(ctx)
	if err != nil {
		panic(err)
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(tid)*int64(100000000)))
	stmts := make([]*sql.Stmt, ps.s.tableCnt)
	for i := 0; i < ps.s.tableCnt; i++ {
		tableName := fmt.Sprintf("sbtest%v", i+1)
		pointSelect := string("SELECT c FROM " + tableName + " WHERE id=?")
		stmt, err := conn.PrepareContext(ctx, pointSelect)
		if err != nil {
			panic(err)
		}
		stmts[i] = stmt
	}

	for i := int64(0); i < ps.s.eventCnt; i++ {
		stmtIndex := r.Intn(ps.s.tableCnt)
		id := r.Int63n(ps.s.tableSize)
		_, err := stmts[stmtIndex].ExecContext(ctx, id)
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("SysbenchPointSelect exec", ps.s.eventCnt)
}

func (ps *SysbenchPointSelect) Cleanup(tid int) {
	fmt.Println("SysbenchPointSelect Cleanup running...")
	threads := ps.s.threadCnt
	tableCnt := ps.s.tableCnt
	for i := (tid % threads) + 1; i <= threads; i = i + tableCnt {
		query := fmt.Sprintf("drop table if exists sbtest%v", i)
		_, err := ps.s.db.ToSqlDB().Exec(query)
		if err != nil {
			fmt.Println("[Failed]:", query, err)
			panic(err)
		}
	}
	fmt.Println("SysbenchPointSelect cleanup database")
}

func (ps *SysbenchPointSelect) GetSysBench() *sysBench {
	return ps.s
}

type SysbenchUpdateIndex struct {
	s *sysBench
}

func (ui *SysbenchUpdateIndex) ID() string {
	return "SysbenchUpdateIndex"
}
func (ui *SysbenchUpdateIndex) Prepare(tid int) {
	fmt.Println("SysbenchUpdateIndex Prepare running...")

}
func (ui *SysbenchUpdateIndex) Run(ctx context.Context, tid int) {
	fmt.Println("SysbenchUpdateIndex Run running...")

}
func (ui *SysbenchUpdateIndex) Cleanup(tid int) {
	fmt.Println("SysbenchUpdateIndex Clearup running...")

}
func (ui *SysbenchUpdateIndex) GetSysBench() *sysBench {
	return ui.s
}

type SysbenchUpdateIndexCreator struct{}

func (creator SysbenchUpdateIndexCreator) Create(s *sysBench) SysbenchWorkload {
	fmt.Println("Sysbench SysbenchUpdateIndex workload creating...")
	w := new(SysbenchUpdateIndex)
	w.s = s
	return w

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
		}
	}
}
func createSysbenchTable(wl SysbenchWorkload, tableId int) {
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
	sqlDB := wl.GetSysBench().db.ToSqlDB()
	dropSql := fmt.Sprintf("drop table if exists sbtest%v", tableId)
	sqlDB.Exec(dropSql)
	_, err := sqlDB.Exec(sql)
	if err != nil {
		panic(err)
	}

	tableSize := wl.GetSysBench().tableSize
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
func prepareSysbenchData(wl SysbenchWorkload, tid int) {
	sysbench := wl.GetSysBench()
	threads := sysbench.threadCnt
	for i := (tid % threads) + 1; i <= threads; i = i + sysbench.tableCnt {
		createSysbenchTable(wl, i)
	}
}

func init() {
	ycsb.RegisterWorkloadCreator("sysbench", sysBenchCreator{})
	RegisterSysbenchWorkloadCreator(SysbenchType_oltp_point_select, SysbenchPointSelectCreator{})
	RegisterSysbenchWorkloadCreator(SysbenchType_oltp_update_index, SysbenchUpdateIndexCreator{})
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

		for {
			select {
			case <-t.C:
				measurement.Output()
			case <-measureCtx.Done():
				return
			}
		}
	}()

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
