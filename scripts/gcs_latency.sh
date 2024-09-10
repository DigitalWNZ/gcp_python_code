#!/bin/bash

echo "Log back trace between $1 and $2"

cluster_name=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
zone=us-central1-a
timestamp=$(date +"%Y%m%d_%H%M%S")  # Format: YYYYMMDD_HHMMSS

for i in {0..7}; do
    echo "Number: $i"
    filename=$cluster_name-w-$i-$timestamp.txt
    echo "FileName: $filename"
    gcloud compute ssh $cluster_name-w-$i --zone $zone --project wep-dev --command="sudo mkdir -p /home/${USER}"
    gcloud compute ssh $cluster_name-w-$i --zone $zone --project wep-dev --command="sudo chmod -R a+rwx /home/${USER}"
    gcloud compute ssh $cluster_name-w-$i --zone $zone --project wep-dev --command="sudo journalctl -u hbase-regionserver --since '$1' --until '$2' | grep "gcsFSRead" | cut -d':' -f13 | cut -d'}' -f1 | more > /home/${USER}/$filename"
    gcloud compute ssh $cluster_name-w-$i --zone $zone --project wep-dev --command="sudo -u root gsutil cp /home/${USER}/$filename gs://shein-hbase-anywhere-cache-test/ycsb_hbase_result/gcs_log/"
done

python gcs_log.py $timestamp $cluster_name