package ydb

// ydb properties
const (
	ydbDSN        = "ydb.dsn"
	ydbDSNDefault = "grpc://localhost:2136/local"

	ydbCompression        = "ydb.compression"
	ydbCompressionDefault = false

	ydbAutoPartitioning        = "ydb.auto.partitioning"
	ydbAutoPartitioningDefault = true

	ydbMaxPartSizeMb        = "ydb.max.part.size.mb"
	ydbMaxPartSizeMbDefault = int64(2000) // 2GB

	ydbMaxPartCount        = "ydb.max.parts.count"
	ydbMaxPartCountDefault = int64(50)

	ydbSplitByLoad        = "ydb.split.by.load"
	ydbSplitByLoadDefault = true

	ydbSplitBySize        = "ydb.split.by.size"
	ydbSplitBySizeDefault = true

	ydbForceUpsert        = "ydb.force.upsert"
	ydbForceUpsertDefault = false

	ydbDriverType        = "ydb.driver.type"
	ydbDriverTypeNative  = "native"
	ydbDriverTypeSql     = "database/sql"
	ydbDriverTypeDefault = ydbDriverTypeNative

	ydbDriversCount        = "ydb.drivers.count"
	ydbDriversCountDefault = 10
)
