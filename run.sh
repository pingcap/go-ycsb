#!/usr/bin/env bash

# Entrypoint script to run the workload

TEST_DB="ycsb_tigris"

tigris list databases | grep ${TEST_DB}
if [ $? -ne 0 ]
then
	/go-ycsb load tigris -p tigris.host=tigris-http -p tigris.port=80 -P workloads/conta -p threadcount=1
fi
	while true
	do
		/go-ycsb run tigris -p tigris.host=tigris-http -p tigris.port=80 -P workloads/conta -p threadcount=4
	done
