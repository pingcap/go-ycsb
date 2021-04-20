package workload

import (
	"context"
	"fmt"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type sysBench struct {
	p *properties.Properties
}

func (s *sysBench) Init(db ycsb.DB) error {
	fmt.Println("sysBench Init running...")
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

func init() {
	ycsb.RegisterWorkloadCreator("sysbench", sysBenchCreator{})
}
