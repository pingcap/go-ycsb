#!/usr/bin/env bash

# Entrypoint script to run the workload

TIGRIS_HOST=tigris-http
TIGRIS_PORT=80
if [ -n "$TIGRIS_URL" ]; then
	HASPORT=$(echo "$TIGRIS_URL" | grep ':' | wc -l)
	if [ "$HASPORT" -eq 0 ]; then
		echo "incorrectly formatted TIGRIS_URL $TIGRIS_URL"
		exit 1
	fi
	TIGRIS_HOST=$(echo "$TIGRIS_URL" | cut -d: -f1)
	TIGRIS_PORT=$(echo "$TIGRIS_URL" | cut -d: -f2)
fi 

TEST_DB="${TEST_DB:-ycsb_tigris}"
RECORDCOUNT=${RECORDCOUNT:-1000000} # 1G database
OPERATIONCOUNT=${OPERATIONCOUNT:-10000000}
READALLFIELDS=${READALLFIELDS:-true}
READPROPORTION=${READPROPORTION:-0.5}
UPDATEPROPORTION=${UPDATEPROPORTION:-0.5}
SCANPROPORTION=${SCANPROPORTION:-0} # Scans are not supported yet
INSERTPROPORTION=${INSERTPROPORTION:-0}
REQUESTDISTRIBUTION=${REQUESTDISTRIBUTION:-uniform}
LOADTHREADCOUNT=${LOADTHREADCOUNT:-1}
RUNTHREADCOUNT=${RUNTHREADCOUNT:-16}

WORKLOAD="recordcount=${RECORDCOUNT}
operationcount=${OPERATIONCOUNT}
workload=core

readallfields=${READALLFIELDS}

readproportion=${READPROPORTION}
updateproportion=${UPDATEPROPORTION}
scanproportion=${SCANPROPORTION}
insertproportion=${INSERTPROPORTION}

requestdistribution=${REQUESTDISTRIBUTION}"

echo "${WORKLOAD}" > workloads/dynamic

tigris list databases | grep ${TEST_DB} > /dev/null 2>&1
DB_EXISTS=$?

if [ ${DB_EXISTS} -eq 0 ]
then
	echo "No database is available for benchmarking, please create and load ${TEST_DB}"
	sleep 3600
fi

while true
do
	/go-ycsb run tigris -p tigris.host="$TIGRIS_HOST" -p tigris.port="$TIGRIS_PORT" -p tigris.dbname="$TEST_DB" -P workloads/dynamic -p threadcount=${RUNTHREADCOUNT}
done
