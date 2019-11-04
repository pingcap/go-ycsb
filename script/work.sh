# /bin/bash
$1 run mysql -P ./../workloads/workload_zyh -p mysql.host=$2 -p mysql.port=$3 -p usertable=$4 -p threadcount=128 >> ./table1.log &
sleep 900
$1 run mysql -P ./../workloads/workload_zyh -p mysql.host=$2 -p mysql.port=$3 -p usertable=$5 -p threadcount=128 >> ./table2.log &
sleep 900
$1 run mysql -P ./../workloads/workload_zyh -p mysql.host=$2 -p mysql.port=$3 -p usertable=$6 -p threadcount=128 >> ./table3.log &
sleep 900
$1 run mysql -P ./../workloads/workload_zyh -p mysql.host=$2 -p mysql.port=$3 -p usertable=$7 -p threadcount=128 >> ./table4.log &
