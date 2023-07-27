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
	// Used if fieldlengthdistribution is "histogram"
	FieldLengthHistogramFile         = "fieldlengthhistogram"
	FieldLengthHistogramFileDefault  = "hist.txt"
	ReadAllFields                    = "readallfields"
	ReadALlFieldsDefault             = true
	WriteAllFields                   = "writeallfields"
	WriteAllFieldsDefault            = false
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

	FluctuateLoad                    = "fluctuateload"
	FluctuateLoadDefault             = false
	FluctuateLowLoadInterval         = "fluctuatelowloadinterval"
	FluctuateLowLoadIntervalDefault  = 50
	FluctuatePeakLoadInterval        = "fluctuatepeakloadinterval"
	FluctuatePeakLoadIntervalDefault = 10
	FluctuateLoadRatio               = "fluctuateloadratio"
	FluctuateLoadRatioDefault        = 10
	FluctuateSineA                   = "fluctuatesinea"
	FluctuateSineADefault            = 500.0
	FluctuateSineB                   = "fluctuatesineb"
	FluctuateSineBDefault            = 0.05
	FluctuateSineC                   = "fluctuatesinec"
	FluctuateSineCDefault            = 0.0
	FluctuateSineD                   = "fluctuatesined"
	FluctuateSineDDefault            = 500.0

	WaitingQueue            = "waitingqueue"
	WaitingQueueDefault     = false
	WaitingQueueSize        = "waitingqueuesize"
	WaitingQueueSizeDefault = 10000

	NeedShuffleRegion            = "needshuffleregion"
	NeedShuffleRegionDefault     = false
	ShuffleRegionInterval        = "shuffleregioninterval"
	ShuffleRegionIntervalDefault = 240

	TraceReplay        = "tracereplay"
	TraceReplayDefault = false
	ReplayThreadCount  = "replaythreadcount"
	ApplyThreadCount   = "applythreadcount"

	KvSizeUseParetoDistInRegion = "kvSizeUseParetoDistInRegion"

	MixGraphBench        = "mixgraph"
	MixGraphBenchDefault = false
	MixGetRatio          = "mixgetratio"
	MixGetRatioDefault   = float64(0.83)
	MixPutRatio          = "mixputratio"
	MixPutRatioDefault   = float64(0.14)
	MixSeekRatio         = "mixseekratio"
	MixSeekRatioDefault  = float64(0.03)
	KeyRangeNum          = "keyrangenum"
	KeyRangeNumDefault   = int64(30)
	KeyRangeDistA        = "keyrangedista"
	KeyRangeDistADefault = float64(14.18)
	KeyRangeDistB        = "keyrangedistb"
	KeyRangeDistBDefault = float64(-2.917)
	KeyRangeDistC        = "keyrangedistc"
	KeyRangeDistCDefault = float64(0.0164)
	KeyRangeDistD        = "keyrangedistd"
	KeyRangeDistDDefault = float64(-0.08082)
	KeyDistA             = "keydista"
	KeyDistADefault      = float64(0.002312)
	KeyDistB             = "keydistb"
	KeyDistBDefault      = float64(0.3467)
	ValueTheta           = "valuetheta"
	ValueThetaDefault    = float64(0)
	ValueK               = "valuek"
	ValueKDefault        = float64(0.2615)
	ValueSigma           = "valuesigma"
	ValueSigmaDefault    = float64(25.45)
	IterTheta            = "itertheta"
	IterThetaDefault     = float64(0)
	IterK                = "iterk"
	IterKDefault         = float64(2.517)
	IterSigma            = "itersigma"
	IterSigmaDefault     = float64(14.236)
	LogInterval          = "measurement.interval"
)
