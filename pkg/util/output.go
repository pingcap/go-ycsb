package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/olekukonko/tablewriter"
)

// output style
const (
	OutputStylePlain = "plain"
	OutputStyleTable = "table"
	OutputStyleJson  = "json"
)

// RenderString renders headers and values according to the format provided
func RenderString(format string, headers []string, values [][]string) {
	if len(values) == 0 {
		return
	}

	buf := new(bytes.Buffer)
	for _, value := range values {
		args := make([]string, len(headers)-1)
		for i, header := range headers[1:] {
			args[i] = header + ": " + value[i+1]
		}
		buf.WriteString(fmt.Sprintf(format, value[0], strings.Join(args, ", ")))
	}
	fmt.Print(buf.String())
}

// RenderTable will use given headers and values to render a table style output
func RenderTable(headers []string, values [][]string) {
	if len(values) == 0 {
		return
	}
	tb := tablewriter.NewWriter(os.Stdout)
	tb.SetHeader(headers)
	tb.AppendBulk(values)
	tb.Render()
}

// RnederJson will combine the headers and values and print a json string
func RenderJson(headers []string, values [][]string) {
	if len(values) == 0 {
		return
	}
	data := make([]map[string]string, 0, len(values))
	for _, value := range values {
		line := make(map[string]string, 0)
		for i, header := range headers {
			line[header] = value[i]
		}
		data = append(data, line)
	}
	outStr, err := json.Marshal(data)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(outStr))
}

// IntToString formats int value to string
func IntToString(i interface{}) string {
	return fmt.Sprintf("%d", i)
}

// FloatToOneString formats float into string with one digit after dot
func FloatToOneString(f interface{}) string {
	return fmt.Sprintf("%.1f", f)
}
