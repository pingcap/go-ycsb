function exec_workload() {
  workload=${1:-workloads/workloada}
  db_type=${2:-mysql}
  params=$(get_db_params "$db_type")
  ./bin/go-ycsb load "$params" -P "$workload" && ./bin/go-ycsb run "$params" -P "$workload"
}

function mysql_params() {
  host=${MYSQL_HOST:-127.0.0.1}
  port=${MYSQL_PORT:-4000}
  echo "mysql -p mysql.host=${host} -p mysql.port=${port}"
}

function spanner_params() {
  spanner_db=${SPANNER_DB:-projects/pingcap-gardener/instances/colopl-test/databases/test}
  echo "-p spanner.db=$spanner_db"
}

function get_db_params() {
  db_type=${1:-mysql}
  if [[ $db_type == "mysql" ]]; then
    mysql_params
  elif [[ $db_type == "spanner" ]]; then
    spanner_params
  fi
}

workload=${1:-workloads/workloada}
db_type=${2:-mysql}

exec_workload $workload $db_type

