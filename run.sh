#!/usr/bin/env bash

# Entrypoint script to run the workload

TEST_DB="ycsb_tigris"
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

while [ ${DB_EXISTS} -eq 0 ]
do
	echo "Attempting to drop the existing database"
	tigris drop database ${TEST_DB}
	tigris list databases | grep ${TEST_DB} > /dev/null 2>&1
	DB_EXISTS=$?
done

/go-ycsb load tigris -p tigris.host=tigris-http -p tigris.port=80 -P workloads/dynamic -p threadcount=${LOADTHREADCOUNT}

while true
do
	/go-ycsb run tigris -p tigris.host=tigris-http -p tigris.port=80 -P workloads/dynamic -p threadcount=${RUNTHREADCOUNT}
done
