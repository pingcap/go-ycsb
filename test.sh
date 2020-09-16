function exec_workload() {
  workload=${1:-workloads/workloada}
  ./bin/go-ycsb load $(mysql_params) -P "$workload" && ./bin/go-ycsb run $(mysql_params) -P "$workload"
}

function mysql_params() {
  host=${MYSQL_HOST:-127.0.0.1}
  port=${MYSQL_PORT:-4000}
  echo "mysql -p mysql.host=${host} -p mysql.port=${port}"
}

exec_workload $1
