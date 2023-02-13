package measurement

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
)

type histograms struct {
	p *properties.Properties

	histograms map[string]*histogram
}

func (h *histograms) GenerateExtendedOutputs() {
	exportHistograms := h.p.GetBool(prop.MeasurementHistogramPercentileExport, prop.MeasurementHistogramPercentileExportDefault)
	if exportHistograms {
		exportHistogramsFilepath := h.p.GetString(prop.MeasurementHistogramPercentileExportFilepath, prop.MeasurementHistogramPercentileExportFilepathDefault)
		for op, opM := range h.histograms {
			outFile := fmt.Sprintf("%s%s-percentiles.txt", exportHistogramsFilepath, op)
			fmt.Printf("Exporting the full latency spectrum for operation '%s' in percentile output format into file: %s.\n", op, outFile)
			f, err := os.Create(outFile)
			if err != nil {
				panic("failed to create percentile output file: " + err.Error())
			}
			defer f.Close()
			w := bufio.NewWriter(f)
			_, err = opM.hist.PercentilesPrint(w, 1, 1.0)
			w.Flush()
			if err != nil {
				panic("failed to print percentiles: " + err.Error())
			}
		}
	}
}

func (h *histograms) Measure(op string, start time.Time, lan time.Duration) {
	opM, ok := h.histograms[op]
	if !ok {
		opM = newHistogram()
		h.histograms[op] = opM
	}

	opM.Measure(lan)
}

func (h *histograms) summary() map[string][]string {
	summaries := make(map[string][]string, len(h.histograms))
	for op, opM := range h.histograms {
		summaries[op] = opM.Summary()
	}
	return summaries
}

func (h *histograms) Summary() {
	h.Output(os.Stdout)
}

func (h *histograms) Output(w io.Writer) error {
	summaries := h.summary()
	keys := make([]string, 0, len(summaries))
	for k := range summaries {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	lines := [][]string{}
	for _, op := range keys {
		line := []string{op}
		line = append(line, summaries[op]...)
		lines = append(lines, line)
	}

	outputStyle := h.p.GetString(prop.OutputStyle, util.OutputStylePlain)
	switch outputStyle {
	case util.OutputStylePlain:
		util.RenderString(w, "%-6s - %s\n", header, lines)
	case util.OutputStyleJson:
		util.RenderJson(w, header, lines)
	case util.OutputStyleTable:
		util.RenderTable(w, header, lines)
	default:
		panic("unsupported outputstyle: " + outputStyle)
	}
	return nil
}

func InitHistograms(p *properties.Properties) *histograms {
	return &histograms{
		p:          p,
		histograms: make(map[string]*histogram, 16),
	}
}
