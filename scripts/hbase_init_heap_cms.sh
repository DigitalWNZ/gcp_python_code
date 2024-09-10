#!/bin/bash

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
HBASE_OFFHEAPSIZE=$(/usr/share/google/get_metadata_value attributes/hbase-offheap-size)
HBASE_HEAPSIZE=$(/usr/share/google/get_metadata_value attributes/hbase-heap-size)

if [[ "${ROLE}" == 'Worker' ]]; then
  [[ ! -z "${HBASE_OFFHEAPSIZE}" ]] && echo "export HBASE_OFFHEAPSIZE=${HBASE_OFFHEAPSIZE}" >> /etc/hbase/conf/hbase-env.sh
  [[ ! -z "${HBASE_HEAPSIZE}" ]] && echo "export HBASE_REGIONSERVER_OPTS='-Xms${HBASE_HEAPSIZE} -Xmx${HBASE_HEAPSIZE} -Xmn3g -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+UseCMSCompactAtFullCollection -XX:CMSFullGCsBeforeCompaction=15 -XX:CMSInitiatingOccupancyFraction=70 -XX:ParallelGCThreads=16 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/root/gc-hbase.log'" >> /etc/hbase/conf/hbase-env.sh
  systemctl restart hbase-regionserver
fi