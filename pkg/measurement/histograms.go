package measurement

import (
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type histograms struct {
	p *properties.Properties

	histograms map[string]*histogram
}

func (h *histograms) Measure(op string, start time.Time, lan time.Duration) {
	opM, ok := h.histograms[op]
	if !ok {
		opM = newHistogram(h.p)
		h.histograms[op] = opM
	}

	opM.Measure(lan)
}

func (h *histograms) Summary() map[string][]string {
	summaries := make(map[string][]string, len(h.histograms))
	for op, opM := range h.histograms {
		summaries[op] = opM.Summary()
	}
	return summaries
}

func (h *histograms) Info() map[string]ycsb.MeasurementInfo {
	opMeasurementInfo := make(map[string]ycsb.MeasurementInfo, len(h.histograms))
	for op, opM := range h.histograms {
		opMeasurementInfo[op] = opM.Info()
	}
	return opMeasurementInfo
}

func (h *histograms) OpNames() []string {
	opNames := make([]string, 0, len(h.histograms))
	for op := range h.histograms {
		opNames = append(opNames, op)
	}
	return opNames
}

func InitHistograms(p *properties.Properties) *histograms {
	return &histograms{
		p:          p,
		histograms: make(map[string]*histogram, 16),
	}
}
