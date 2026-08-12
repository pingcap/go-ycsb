package workload

import (
	"context"
	"testing"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
)

// The zipfian key chooser's range is extended beyond the loaded records by
// the expected number of run-phase inserts (expectedNewKeys). nextKeyNum
// must not return key numbers from the extension before they are actually
// inserted and acknowledged; otherwise reads and updates target keys that
// were never inserted. Mirrors the guard in upstream YCSB
// CoreWorkload.nextKeynum.
func TestZipfianNextKeyNumStaysWithinAcknowledgedRange(t *testing.T) {
	p := properties.NewProperties()
	p.Set(prop.RecordCount, "1000")
	p.Set(prop.OperationCount, "10000")
	p.Set(prop.RequestDistribution, "zipfian")
	p.Set(prop.ReadProportion, "0.5")
	p.Set(prop.UpdateProportion, "0")
	p.Set(prop.ScanProportion, "0")
	p.Set(prop.InsertProportion, "0.5")

	wl, err := coreCreator{}.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	c := wl.(*core)
	ctx := c.InitThread(context.Background(), 0, 1)
	state := ctx.Value(stateKey).(*coreState)

	last := c.transactionInsertKeySequence.Last()
	violations := 0
	const draws = 200000
	for i := 0; i < draws; i++ {
		if kn := c.nextKeyNum(state); kn > last {
			violations++
		}
	}
	if violations > 0 {
		t.Fatalf("nextKeyNum returned a not-yet-inserted key %d/%d times (acknowledged Last=%d)",
			violations, draws, last)
	}
}
