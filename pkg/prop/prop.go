// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package prop

// Properties
const (
	InsertStart        = "insertstart"
	InsertCount        = "insertcount"
	InsertStartDefault = int64(0)

	OperationCount     = "operationcount"
	RecordCount        = "recordcount"
	RecordCountDefault = int64(0)
	Workload           = "workload"
	DB                 = "db"
	Exporter           = "exporter"
	ExportFile         = "exportfile"
	ThreadCount        = "threadcount"
	ThreadCountDefault = int64(200)
	Target             = "target"
	MaxExecutiontime   = "maxexecutiontime"
	WarmUpTime         = "warmuptime"
	DoTransactions     = "dotransactions"
	Status             = "status"
	Label              = "label"
	// batch mode
	BatchSize        = "batch.size"
	DefaultBatchSize = int(1)

	TableName         = "table"
	TableNameDefault  = "usertable"
	FieldCount        = "fieldcount"
	FieldCountDefault = int64(10)
	// "uniform", "zipfian", "constant", "histogram"
	FieldLengthDistribution        = "fieldlengthdistribution"
	FieldLengthDistributionDefault = "constant"
	FieldLength                    = "fieldlength"
	FieldLengthDefault             = int64(100)
	// Lower bound for "uniform"/"zipfian" fieldlengthdistribution. Lets a
	// workload model field sizes that vary within a range (e.g. 8-24 bytes)
	// instead of always starting at 1 byte.
	FieldLengthMinimum        = "fieldlengthminimum"
	FieldLengthMinimumDefault = int64(1)
	// Used if fieldlengthdistribution is "histogram"
	FieldLengthHistogramFile        = "fieldlengthhistogram"
	FieldLengthHistogramFileDefault = "hist.txt"
	ReadAllFields                   = "readallfields"
	ReadALlFieldsDefault            = true
	WriteAllFields                  = "writeallfields"
	WriteAllFieldsDefault           = false
	// FieldNamePrefix/FieldNameStartIndex control how the generated field
	// names (field0, field1, ...) are built, e.g. fieldnameprefix=feature_
	// fieldnamestartindex=1 produces feature_1..feature_N.
	FieldNamePrefix            = "fieldnameprefix"
	FieldNamePrefixDefault     = "field"
	FieldNameStartIndex        = "fieldnamestartindex"
	FieldNameStartIndexDefault = int64(0)
	// LastFieldName, if set, overrides the name of the final generated field.
	// Useful for modeling a trailing metadata column alongside numbered
	// feature fields, e.g. lastfieldname=event_ts.
	LastFieldName        = "lastfieldname"
	LastFieldNameDefault = ""
	// FieldValueType controls the CONTENT of generated field values, not
	// just their size. Feature-store fields are typically typed scalars
	// (Feast: INT32/INT64/FLOAT32/FLOAT64/BOOL/STRING/UNIX_TIMESTAMP;
	// Featureform: Int/Int32/Int64/Float32/Float64/Bool/String/Timestamp)
	// rather than opaque bytes, so a workload can opt into realistic typed
	// content instead of the default random-byte payload.
	// One of: "random" (default, opaque random bytes), "numeric" (a mix of
	// integer/float/boolean scalars), "integer", "float", "boolean",
	// "timestamp" (RFC3339, e.g. for an event-time metadata field).
	FieldValueType        = "fieldvaluetype"
	FieldValueTypeDefault = "random"
	// LastFieldValueType, if set, overrides the value type of the final
	// generated field (see LastFieldName), e.g. lastfieldvaluetype=timestamp
	// to give a trailing event_ts field realistic RFC3339 content while
	// numbered feature fields stay numeric.
	LastFieldValueType        = "lastfieldvaluetype"
	LastFieldValueTypeDefault = ""
	// Magnitude/precision for "integer" and "float" fieldvaluetype content.
	// Defaults model the shape of typical feature-store scalars - e.g.
	// Feast's driver-ranking demo features conv_rate/acc_rate (floats in
	// [0,1)) and avg_daily_trips (a small bounded count) - rather than
	// digit-filling to a target byte length.
	FieldValueIntegerMin        = "fieldvalueintegermin"
	FieldValueIntegerMinDefault = int64(0)
	FieldValueIntegerMax        = "fieldvalueintegermax"
	FieldValueIntegerMaxDefault = int64(100000)
	FieldValueFloatMin          = "fieldvaluefloatmin"
	FieldValueFloatMinDefault   = float64(0.0)
	FieldValueFloatMax          = "fieldvaluefloatmax"
	FieldValueFloatMaxDefault   = float64(1.0)
	// Decimal places for "float" fieldvaluetype content, e.g. "0.8472".
	FieldValueFloatPrecision         = "fieldvaluefloatprecision"
	FieldValueFloatPrecisionDefault  = int64(4)
	DataIntegrity                    = "dataintegrity"
	DataIntegrityDefault             = false
	ReadProportion                   = "readproportion"
	ReadProportionDefault            = float64(0.95)
	UpdateProportion                 = "updateproportion"
	UpdateProportionDefault          = float64(0.05)
	InsertProportion                 = "insertproportion"
	InsertProportionDefault          = float64(0.0)
	ScanProportion                   = "scanproportion"
	ScanProportionDefault            = float64(0.0)
	ReadModifyWriteProportion        = "readmodifywriteproportion"
	ReadModifyWriteProportionDefault = float64(0.0)
	// "uniform", "zipfian", "latest"
	RequestDistribution        = "requestdistribution"
	RequestDistributionDefault = "uniform"
	ZeroPadding                = "zeropadding"
	ZeroPaddingDefault         = int64(1)
	MinScanLength              = "minscanlength"
	MinScanLengthDefault       = int64(1)
	MaxScanLength              = "maxscanlength"
	MaxScanLengthDefault       = int64(1000)
	// "uniform", "zipfian"
	ScanLengthDistribution        = "scanlengthdistribution"
	ScanLengthDistributionDefault = "uniform"
	// "ordered", "hashed"
	InsertOrder                   = "insertorder"
	InsertOrderDefault            = "hashed"
	HotspotDataFraction           = "hotspotdatafraction"
	HotspotDataFractionDefault    = float64(0.2)
	HotspotOpnFraction            = "hotspotopnfraction"
	HotspotOpnFractionDefault     = float64(0.8)
	InsertionRetryLimit           = "core_workload_insertion_retry_limit"
	InsertionRetryLimitDefault    = int64(0)
	InsertionRetryInterval        = "core_workload_insertion_retry_interval"
	InsertionRetryIntervalDefault = int64(3)

	ExponentialPercentile        = "exponential.percentile"
	ExponentialPercentileDefault = float64(95)
	ExponentialFrac              = "exponential.frac"
	ExponentialFracDefault       = float64(0.8571428571)

	DebugPprof        = "debug.pprof"
	DebugPprofDefault = ":6060"

	Verbose         = "verbose"
	VerboseDefault  = false
	DropData        = "dropdata"
	DropDataDefault = false

	Silence        = "silence"
	SilenceDefault = true

	KeyPrefix        = "keyprefix"
	KeyPrefixDefault = "user"

	LogInterval = "measurement.interval"

	MeasurementType          = "measurementtype"
	MeasurementTypeDefault   = "histogram"
	MeasurementRawOutputFile = "measurement.output_file"

	Command = "command"

	OutputStyle = "outputstyle"

	// MeasurementHistogramPercentileExport properties -- related to histogram latencies exporting
	MeasurementHistogramPercentileExport                = "histogram.percentiles.export"
	MeasurementHistogramPercentileExportDefault         = false
	MeasurementHistogramPercentileExportFilepath        = "histogram.percentiles.export.filepath"
	MeasurementHistogramPercentileExportFilepathDefault = "./"
)
