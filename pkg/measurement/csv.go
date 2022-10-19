package measurement

import (
	"fmt"
	"io"
	"time"

	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type csventry struct {
	// start time of the operation in us from unix epoch
	start_us int64
	// latency of the operation in us
	latency_us int64
}

type csvs struct {
	csvs map[string][]csventry
}

func InitCSV() *csvs {
	return &csvs{
		csvs: make(map[string][]csventry, 16),
	}
}

func (c *csvs) Info() map[string]ycsb.MeasurementInfo {
	info := make(map[string]ycsb.MeasurementInfo, len(c.csvs))
	for op, _ := range c.csvs {
		info[op] = nil
	}
	return info
}

func (c *csvs) Measure(op string, start time.Time, lan time.Duration) {
	c.csvs[op] = append(c.csvs[op], csventry{
		start_us:   start.UnixMicro(),
		latency_us: lan.Microseconds(),
	})
}

func (c *csvs) Output(w io.Writer) error {
	_, err := fmt.Fprintln(w, "operation,timestamp_us,latency_us")
	if err != nil {
		return err
	}
	for op, entries := range c.csvs {
		for _, entry := range entries {
			_, err := fmt.Fprintf(w, "%s,%d,%d\n", op, entry.start_us, entry.latency_us)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *csvs) OpNames() []string {
	names := make([]string, 0, len(c.csvs))
	for op := range c.csvs {
		names = append(names, op)
	}
	return names
}
