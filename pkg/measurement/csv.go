package measurement

import (
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type csventry struct {
	op    string
	start time.Time
	lan   time.Duration
}

type csv struct {
	p    *properties.Properties
	data []csventry
}

func InitCSV(p *properties.Properties) *csv {
	return &csv{
		p: p,
	}
}

func (c *csv) Info() map[string]ycsb.MeasurementInfo {
	opMeasurementInfo := make(map[string]ycsb.MeasurementInfo)
	return opMeasurementInfo
}

func (c *csv) Measure(op string, start time.Time, lan time.Duration) {
	c.data = append(c.data, csventry{op, start, lan})
}

func (c *csv) Summary() map[string][]string {
	summaries := make(map[string][]string)
	return summaries
}

func (c *csv) OpNames() []string {
	opNames := make([]string, 0)
	return opNames
}
