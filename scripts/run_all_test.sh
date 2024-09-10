#!/bin/bash

/root/ycsb_run.sh read 350gb 1024 0
/root/ycsb_run.sh read 350gb 1024 0

since=$(date +"%Y-%m-%d %H:%M:%S")
/root/ycsb_run.sh read 350gb 64 0
until=$(date +"%Y-%m-%d %H:%M:%S")
/root/gcs_latency.sh "$since" "$until" >> gcs_latency_log.txt

sleep 120

since=$(date +"%Y-%m-%d %H:%M:%S")
/root/ycsb_run.sh read 350gb 128 0
until=$(date +"%Y-%m-%d %H:%M:%S")
/root/gcs_latency.sh "$since" "$until" >> gcs_latency_log.txt

sleep 120

since=$(date +"%Y-%m-%d %H:%M:%S")
/root/ycsb_run.sh read 350gb 256 0
until=$(date +"%Y-%m-%d %H:%M:%S")
/root/gcs_latency.sh "$since" "$until" >> gcs_latency_log.txt

sleep 120

since=$(date +"%Y-%m-%d %H:%M:%S")
/root/ycsb_run.sh read 350gb 512 0
until=$(date +"%Y-%m-%d %H:%M:%S")
/root/gcs_latency.sh "$since" "$until" >> gcs_latency_log.txt

sleep 120

since=$(date +"%Y-%m-%d %H:%M:%S")
/root/ycsb_run.sh read 350gb 1024 0
until=$(date +"%Y-%m-%d %H:%M:%S")
/root/gcs_latency.sh "$since" "$until" >> gcs_latency_log.txt

gsutil cp /root/nohup.out gs://shein-hbase-anywhere-cache-test/ycsb_hbase_result/$(hostname -s)/
gsutil cp /root/gcs_latency_log.txt gs://shein-hbase-anywhere-cache-test/ycsb_hbase_result/$(hostname -s)/