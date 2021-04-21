package workload

import (
	"context"
	"fmt"

	"github.com/magiconair/properties"
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
	p *properties.Properties
}

// Sysbench workload has nothing todo here
func (s *sysBench) Init(db ycsb.DB) error {
	fmt.Println("sysBench Init running...")
	return nil
}

func (c *sysBench) Exec(ctx context.Context, db ycsb.DB) error {
	cmdType := c.p.GetString(prop.SysbenchCmdType, "nil")
	workloadType := c.p.GetString(prop.SysbenchWorkLoadType, "nil")
	creator := GetSysbenchWorkloadCreator(workloadType)
	sysBenchWL, err := creator.Create(c.p)
	if err != nil {
		panic(err)
	}
	switch cmdType {
	case "prepare":
		fmt.Println(sysBenchWL.ID(), "exec prepare...")
	case "run":
		fmt.Println(sysBenchWL.ID(), "exec run...")
	case "cleanup":
		fmt.Println(sysBenchWL.ID(), "exec cleanup...")
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

func (sysBenchCreator) Create(p *properties.Properties) (ycsb.Workload, error) {
	sysbench := new(sysBench)
	sysbench.p = p
	return sysbench, nil
}

type SysbenchPointSelectCreator struct{}
type sysbenchWorkloadCreator interface {
	Create(p *properties.Properties) (SysbenchWorkload, error)
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

func (creator SysbenchPointSelectCreator) Create(p *properties.Properties) (SysbenchWorkload, error) {
	fmt.Println("Sysbench SysbenchPointSelect workload creating...")
	w := new(SysbenchPointSelect)
	w.p = p
	return w, nil
}

type SysbenchWorkload interface {
	ID() string
	// prepare the base data for the workload test
	Prepare() error
	// exec the workload test
	Run() error
	// clean the base data
	Cleanup() error
}

type SysbenchPointSelect struct {
	p *properties.Properties
}

func (ps *SysbenchPointSelect) ID() string {
	return "SysbenchPointSelect"
}
func (ps *SysbenchPointSelect) Prepare() error {
	fmt.Println("SysbenchPointSelect Prepare running ...")
	return nil
}

func (ps *SysbenchPointSelect) Run() error {
	fmt.Println("SysbenchPointSelect Run running...")
	return nil
}

func (ps *SysbenchPointSelect) Cleanup() error {
	fmt.Println("SysbenchPointSelect Cleanup running...")
	return nil
}

type SysbenchUpdateIndex struct {
	p *properties.Properties
}

func (ui *SysbenchUpdateIndex) ID() string {
	return "SysbenchUpdateIndex"
}
func (ui *SysbenchUpdateIndex) Prepare() error {
	fmt.Println("SysbenchUpdateIndex Prepare running...")
	return nil
}
func (ui *SysbenchUpdateIndex) Run() error {
	fmt.Println("SysbenchUpdateIndex Run running...")
	return nil
}
func (ui *SysbenchUpdateIndex) Cleanup() error {
	fmt.Println("SysbenchUpdateIndex Clearup running...")
	return nil
}

type SysbenchUpdateIndexCreator struct{}

func (creator SysbenchUpdateIndexCreator) Create(p *properties.Properties) (SysbenchWorkload, error) {
	fmt.Println("Sysbench SysbenchUpdateIndex workload creating...")
	w := new(SysbenchUpdateIndex)
	w.p = p
	return w, nil

}

func init() {
	ycsb.RegisterWorkloadCreator("sysbench", sysBenchCreator{})
	RegisterSysbenchWorkloadCreator(SysbenchType_oltp_point_select, SysbenchPointSelectCreator{})
	RegisterSysbenchWorkloadCreator(SysbenchType_oltp_update_index, SysbenchUpdateIndexCreator{})
}
