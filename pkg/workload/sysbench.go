package workload

import (
	"context"
	"fmt"
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
	p  *properties.Properties
	db ycsb.DB
}

// All the interface including params need to be re-design later.
// Sysbench workload has nothing todo here
func (s *sysBench) Init(db ycsb.DB) error {
	fmt.Println("sysBench Init running...")
	return nil
}

//TODO return none will be ok later.
func (c *sysBench) Exec(ctx context.Context, threadID int) error {
	cmdType := c.p.GetString(prop.SysbenchCmdType, "nil")
	workloadType := c.p.GetString(prop.SysbenchWorkLoadType, "nil")
	creator := GetSysbenchWorkloadCreator(workloadType)
	if creator == nil {
		fmt.Println("sysbench workloadtype doesn't exist, please check your command")
		return nil
	}
	sysBenchWL := creator.Create(c)

	switch cmdType {
	case "prepare":
		sysBenchWL.Prepare()
	case "run":
		sysBenchWL.Run()
	case "cleanup":
		sysBenchWL.Cleanup()
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
	sysbench := new(sysBench)
	sysbench.p = p
	sysbench.db = db
	return sysbench, nil
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
	Prepare()
	// run the workload test
	Run()
	// clean the base data
	Cleanup()

	GetSysBench() *sysBench
}

type SysbenchPointSelect struct {
	s *sysBench
}

func (ps *SysbenchPointSelect) ID() string {
	return "SysbenchPointSelect"
}
func (ps *SysbenchPointSelect) Prepare() {
	fmt.Println("SysbenchPointSelect Prepare running ...")
	//create schema
	//generate test data
	createSysbenchTable(ps)

}

func (ps *SysbenchPointSelect) Run() {
	fmt.Println("SysbenchPointSelect Run running...")
}

func (ps *SysbenchPointSelect) Cleanup() {
	fmt.Println("SysbenchPointSelect Cleanup running...")
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
func (ui *SysbenchUpdateIndex) Prepare() {
	fmt.Println("SysbenchUpdateIndex Prepare running...")

}
func (ui *SysbenchUpdateIndex) Run() {
	fmt.Println("SysbenchUpdateIndex Run running...")

}
func (ui *SysbenchUpdateIndex) Cleanup() {
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

func createSysbenchTable(wl SysbenchWorkload) {
	sysbench := wl.GetSysBench()
	threads := sysbench.p.GetInt(prop.SysbenchThreads, prop.SysbenchThreadsDefault)
	fmt.Println("sysbench threads num", threads)
}

func init() {
	ycsb.RegisterWorkloadCreator("sysbench", sysBenchCreator{})
	RegisterSysbenchWorkloadCreator(SysbenchType_oltp_point_select, SysbenchPointSelectCreator{})
	RegisterSysbenchWorkloadCreator(SysbenchType_oltp_update_index, SysbenchUpdateIndexCreator{})
}

/*************************************************************************************/
// sysbenchClient logic
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

type SysbenchWorker struct {
	tid int
	wl  ycsb.Workload
	db  ycsb.DB
}

func (worker *SysbenchWorker) Run(ctx context.Context) {
	worker.wl.Exec(ctx, worker.tid)
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

			w := SysbenchWorker{tid: threadId,
				wl: c.workload,
				db: c.db}
			ctx := c.workload.InitThread(ctx, threadId, threads)
			ctx = c.db.InitThread(ctx, threadId, threads)
			w.Run(ctx)
			c.db.CleanupThread(ctx)
			c.workload.CleanupThread(ctx)
		}(i + 1)
	}
	//c.workload.Exec(ctx)
	wg.Wait()
	measureCancel()
	<-measureCh
}
