#!/bin/bash

TYPE=$1
DB=$2

# Directory to save data
DATA=./data
CMD=../bin/go-ycsb
# Direcotry to save logs
LOG=./log

RECORDCOUNT=100000000
OPERATIONCOUNT=100000000
THREADCOUNT=16
FIELDCOUNT=10
FIELDLENGTH=100

PROPS="-p recordcount=${RECORDCOUNT} \
    -p operationcount=${OPERATIONCOUNT} \
    -p threadcount=${THREADCOUNT} \
    -p fieldcount=${FIELDCOUNT} \
    -p fieldlength=${FIELDLENGTH}"
PROPS+=" ${@:3}"
WORKLOADS=

mkdir -p ${LOG} 

DBDATA=${DATA}/${DB}

if [ ${DB} == 'rocksdb' ]; then 
    PROPS+=" -p rocksdb.dir=${DBDATA}"
    WORKLOADS="-P property/rocksdb"
elif [ ${DB} == 'badger' ]; then 
    PROPS+=" -p badger.dir=${DBDATA}"
    WORKLOADS="-P property/badger"
fi


if [ ${TYPE} == 'load' ]; then 
    echo "clear data before load"
    case ${DB} in 
        rocksdb)
            PROPS+=" -p rocksdb.dropdata=true"
            ;;
        badger)
            PROPS+=" -p badger.dropdata=true"
            ;;
        mysql)
            PROPS+=" -p mysql.droptable=true"
            ;;
        pg)
            PROPS+=" -p pg.droptable=true"
            ;;
        *)
        ;;
    esac
fi 

echo ${TYPE} ${DB} ${WORKLOADS} ${PROPS}

if [ ${TYPE} == 'load' ]; then 
    $CMD load ${DB} ${WORKLOADS} -p=workload=core ${PROPS} | tee ${LOG}/${DB}_load.log
elif [ ${TYPE} == 'run' ]; then
    for workload in a b c d e f 
    do 
        $CMD run ${DB} -P ../workloads/workload${workload} ${WORKLOADS} ${PROPS} | tee ${LOG}/${DB}_run_workload${workload}.log
    done
else
    echo "invalid type ${TYPE}"
    exit 1
fi 

