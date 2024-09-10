#!/bin/bash

MODE=$1
ROW_LENGTH=$2
THREADS=$3
ID=$4
HOSTNAME=$(hostname -s)
WORKLOAD_FILE=/root/ycsb-0.17.0/workloads/workload_${MODE}_${ROW_LENGTH}
GS_PATH=gs://shein-hbase-anywhere-cache-test/ycsb_hbase_result/${HOSTNAME}
RESULT_FILE=/root/result_${MODE}_${ROW_LENGTH}.txt.${ID}
NOWSTR=$(date +%Y_%m_%d___%H_%M_%S)

echo "${NOWSTR}" > ${RESULT_FILE}

for i in {1..5}
do

  FIX=thread.$THREADS.$NOWSTR.$i
  LOG=/root/${HOSTNAME}_${MODE}_${ROW_LENGTH}.log.${FIX}
  GC_LOG=/root/gc_${HOSTNAME}_${MODE}_${ROW_LENGTH}.log.${FIX}
  JAVA_OPTS="-Xmx31g -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationConcurrentTime -XX:+PrintGCApplicationStoppedTime -Xloggc:${GC_LOG} -verbose:gc"

  if [[ "$MODE" == "write" ]]; then
    /root/ycsb-0.17.0/bin/ycsb.sh load hbase20 -P ${WORKLOAD_FILE} \
    -p table=usertable_${ROW_LENGTH}_${ID} -p columnfamily=cf -p hdrhistogram.percentiles=50,90,95,99,99.9,99.99 -p durability=SKIP_WAL \
    -s -threads ${THREADS} > ${LOG}
  elif [[ "$MODE" == "read" ]]; then
        /root/ycsb-0.17.0/bin/ycsb.sh run  hbase20 -P ${WORKLOAD_FILE} \
    -p table=usertable_${ROW_LENGTH}_${ID} -p columnfamily=cf -p hdrhistogram.percentiles=50,90,95,99,99.9,99.99 -p clientbuffering=true \
    -s -threads ${THREADS} > ${LOG}
  else
        echo "MODE must be either 'read' or 'write'"
  fi
  gsutil cp $LOG ${GS_PATH}/

  echo " " >> ${RESULT_FILE}
  echo "#${i}" >> ${RESULT_FILE}
  tail -n +2 $LOG | awk '{print $NF}' >> ${RESULT_FILE}

  if [[ "$MODE" == "write" ]]; then
    break
  fi

done