#!/bin/bash
apt install wget tmux -y
cd /opt
git clone https://github.com/DigitalWNZ/gcp_python_code.git
cd gcp_python_code/tpcds_dataproc
tar -zxvf tpcds-kit.tar.gz
ROOT_DIR=$(/usr/share/google/get_metadata_value attributes/ROOT_DIR)
sed -i "s#<ROOT_DIR>#${ROOT_DIR}#g" datagen.scala
sed -i "s#<ROOT_DIR>#${ROOT_DIR}#g" create_table.scala
