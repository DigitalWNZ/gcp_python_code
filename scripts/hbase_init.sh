#!/bin/bash
GS_PATH=gs://shein-hbase-anywhere-cache-test/ycsb

apt install tmux sysstat htop iftop -y
systemctl stop hadoop-yarn-timelineserver
systemctl stop spark-history-server
systemctl stop google-fluentd


ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)

if [[ "${ROLE}" == 'Master' ]]; then
  cd /root
  wget https://dlcdn.apache.org/hbase/hbase-operator-tools-1.2.0/hbase-operator-tools-1.2.0-bin.tar.gz
  tar -zxvf hbase-operator-tools-1.2.0-bin.tar.gz

  curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz
  tar xfvz ycsb-0.17.0.tar.gz

  gsutil cp $GS_PATH/workload_* /root/ycsb-0.17.0/workloads/
  gsutil cp $GS_PATH/ycsb_run.sh /root/
  gsutil cp $GS_PATH/create_split_table.sh /root/
  chmod +x /root/ycsb_run.sh
  chmod +x /root/create_split_table.sh
fi