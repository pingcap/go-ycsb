package measurement

import (
	"fmt"
	"io"
	"time"
)

type csventry struct {
	// start time of the operation in us from unix epoch
	start_us int64
	// latency of the operation in us
	latency_us int64
}

type csvs struct {
	opCsv map[string][]csventry
}

func InitCSV() *csvs {
	return &csvs{
		opCsv: make(map[string][]csventry),
	}
}

func (c *csvs) Measure(op string, start time.Time, lan time.Duration) {
	c.opCsv[op] = append(c.opCsv[op], csventry{
		start_us:   start.UnixMicro(),
		latency_us: lan.Microseconds(),
	})
}

func (c *csvs) Output(w io.Writer) error {
	_, err := fmt.Fprintln(w, "operation,timestamp_us,latency_us")
	if err != nil {
		return err
	}
	for op, entries := range c.opCsv {
		for _, entry := range entries {
			_, err := fmt.Fprintf(w, "%s,%d,%d\n", op, entry.start_us, entry.latency_us)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
