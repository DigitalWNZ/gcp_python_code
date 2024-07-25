# Spark TPC-DS on Dataproc
TPC-DS is one of the most popular benchmark for OLAP. This guide describes the steps to run TPC-DS on Dataproc.

The demo is delivered in **Linux Shell** scripts. You can start a **Cloud Shell** sesstion to run the scripts.
## Prerequisites

1. VPC including networks, firewall rules, NAT are setup
2. Google APIs (Dataflow, Bigquery, Cloud Storage) are already enabled

## Environment variables

```bash
export CLUSTER_NAME=cluster-tpcds
export PROJECT=agolis-allen-first
export REGION=us-central1
export NETWORK=first-vpc
export SUBNET=first-vpc
export DATAPROC_BUCKET=agolis-allen-first-dataproc-bucket
export ROOT_DIR=gs://agolis-allen-first-tpcds/tpcds3000_zstd
export DPMS_NAME=hms
gcloud config set project ${PROJECT}
```

## Create DPMS

Create a managed hive metastore. Wait for 15-20 minutes to complete.
```bash
gcloud metastore services create ${DPMS_NAME} \
  --location=${REGION} \
  --hive-metastore-version=3.1.2 \
  --tier=developer \
  --network=${NETWORK} \
  --hive-metastore-configs="hive.metastore.warehouse.dir=gs://${DW_BUCKET}/dw"
```

## Create dataproc cluster
```bash
git clone https://github.com/DigitalWNZ/gcp_python_code.git
gsutil cp gcp_python_code/tpcds_dataproc/tpcds_bootstrap.sh gs://${DATAPROC_BUCKET}/bootstrap/

--My version
gcloud dataproc clusters create ${CLUSTER_NAME} \
  --project ${PROJECT} \
  --bucket ${DATAPROC_BUCKET} \
  --region ${REGION} \
  --subnet ${SUBNET} \
  --dataproc-metastore=projects/${PROJECT}/locations/${REGION}/services/${DPMS_NAME} \
  --scopes cloud-platform \
  --enable-component-gateway \
  --image-version 2.2-debian12 \
  --num-masters 1 \
  --num-workers 6 \
  --num-secondary-workers 0 \
  --master-machine-type n2d-standard-8 \
  --master-min-cpu-platform "AMD Milan" \
  --master-boot-disk-type pd-balanced \
  --master-boot-disk-size 500GB \
  --num-master-local-ssds 1 \
  --master-local-ssd-interface NVME \
  --worker-machine-type n2d-highmem-8 \
  --worker-min-cpu-platform "AMD Milan" \
  --worker-boot-disk-type pd-balanced \
  --worker-boot-disk-size 500GB \
  --num-worker-local-ssds 2 \
  --worker-local-ssd-interface NVME \
  --initialization-actions gs://${DATAPROC_BUCKET}/bootstrap/tpcds_bootstrap.sh \
  --metadata ROOT_DIR=${ROOT_DIR} \
  --properties "hive:yarn.log-aggregation-enable=true" \
  --properties "spark:spark.checkpoint.compress=true" \
  --properties "spark:spark.eventLog.compress=true" \
  --properties "spark:spark.eventLog.compression.codec=zstd" \
  --properties "spark:spark.eventLog.rolling.enabled=true" \
  --properties "spark:spark.io.compression.codec=zstd" \
  --properties "spark:spark.sql.parquet.compression.codec=zstd" \
  --properties "spark:spark.dataproc.enhanced.optimizer.enabled=true" \
  --properties "spark:spark.dataproc.enhanced.execution.enabled=true" \
  --properties "spark:spark.history.fs.logDirectory=gs://${DATAPROC_BUCKET}/phs/spark-job-history" \
  --properties "dataproc:dataproc.cluster.caching.enabled=true" 

--Original version from Forrest
gcloud dataproc clusters create ${CLUSTER_NAME} \
  --project ${PROJECT} \
  --bucket ${DATAPROC_BUCKET} \
  --region ${REGION} \
  --subnet ${SUBNET} \
  --dataproc-metastore=projects/${PROJECT}/locations/${REGION}/services/${DPMS_NAME} \
  --no-address \
  --scopes cloud-platform \
  --enable-component-gateway \
  --num-masters 1 \
  --num-workers 2 \
  --num-secondary-workers 0 \
  --master-machine-type n2d-highmem-4 \
  --master-min-cpu-platform "AMD Milan" \
  --master-boot-disk-type pd-balanced \
  --master-boot-disk-size 300GB \
  --image-version 2.1-debian11 \
  --worker-machine-type n2d-highmem-8 \
  --worker-min-cpu-platform "AMD Milan" \
  --worker-boot-disk-type pd-balanced \
  --worker-boot-disk-size 300GB \
  --secondary-worker-type spot \
  --secondary-worker-boot-disk-type pd-balanced \
  --worker-boot-disk-size 300GB \
  --initialization-actions gs://${DATAPROC_BUCKET}/bootstrap/tpcds_bootstrap.sh \
  --metadata ROOT_DIR=${ROOT_DIR} \
  --properties "hive:yarn.log-aggregation-enable=true" \
  --properties "spark:spark.checkpoint.compress=true" \
  --properties "spark:spark.eventLog.compress=true" \
  --properties "spark:spark.eventLog.compression.codec=zstd" \
  --properties "spark:spark.eventLog.rolling.enabled=true" \
  --properties "spark:spark.io.compression.codec=zstd" \
  --properties "spark:spark.sql.parquet.compression.codec=zstd" 
```

## Generate TPC-DS 1000 data

After dataproc cluster is deployed, login with SSH to the master node of the Dataproc cluster. Suggest to run the command in tmux or screen session because it takes several minitues to hours to generate TPC-DS 1000GB data

```bash
cd /opt/gcp_python_code/tpcds_dataproc
spark-shell --jars spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar -I datagen.scala
```

If you want to generate a different data size, change the following variables in datagen.scala before generating data:

- val scaleFactor = "1000"
- val databaseName = "tpcds1000"

Also change the the value of variable `databaseName` in tpcds.scala

Please bear in mind the table will be created in the datagen process.

## Reuse existing data 
If you only have data without corresponding metastore, You need to run the following script to create tables before starting the TPC-DS benchmark. Make sure to change the variables in `create_table.scala` including: rootDir, dsdgenDir, scaleFactor, format, databaseName
```bash
cd /opt/gcp_python_code/tpcds_dataproc
spark-shell \
  --jars spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar \
  --driver-memory 8192M \
  --deploy-mode client \
  --master yarn \
  -I create_table.scala
```

## Run TPC-DS 1000 tests

After TPC-DS data is generated, run the following command to run tests. Suggest to run in tmux or screen session because it takes several minitues to hours to run TPC-DS tests depending on number of executors. Please change `num-executors` according to your worker vcores.

```bash
cd /opt/gcp_python_code/tpcds_dataproc
--My version
spark-shell \
  --jars spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar \
  --deploy-mode client \
  --master yarn \
  --conf spark.sql.files.maxPartitionBytes=1073741824 \
  --conf fs.gs.block.size=1073741824 \
  --conf spark.hadoop.fs.gs.block.size=1073741824 \
  --conf spark.hadoop.mapreduce.outputcommitter.factory.class=org.apache.hadoop.mapreduce.lib.output.DataprocFileOutputCommitterFactory \
  --conf spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs=false \
  --conf spark.dataproc.enhanced.optimizer.enabled=true \
  --conf spark.dataproc.enhanced.execution.enabled=true \
  --conf spark.driver.cores=8 \
  --conf spark.driver.memory=32g \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=6g \
  --conf spark.network.timeout=2000 \
  --conf spark.executor.heartbeatInterval=300s \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.default.parallelism=144 \
  -I tpcds.scala

--Origianl version from Forrest
spark-shell \
  --jars spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar \
  --executor-memory 18971M \
  --executor-cores 4 \
  --driver-memory 8192M \
  --deploy-mode client \
  --master yarn \
  --num-executors 10 \
  --conf spark.dynamicAllocation.enabled=false \
  -I tpcds.scala
```

## Parse benchmark result
By default, the benchmark result will be stored in ```/results/timestamp=1721266172526``` on hdfs system. The file name like part-00000-303f2546-5580-42f2-947d-eb0dca8f312e-c000.json\
Run the following 2 commands to copy the file to localDisk or GCS.
```
hdfs dfs -ls /results/timestamp=1721891697851
sudo hdfs dfs -copyToLocal /results/timestamp=1721891697851/* .
sudo mv part-00000-5c2c99e7-d98b-4976-bdcd-d7d412f2ef88-c000.json dp_1T2c26g.json
gsutil cp dp_1T2c26g.json gs://agolis-allen-first-tpcds
```
then copy the json file to VM where the data will be parsed. 
```
gsutil cp  gs://agolis-allen-first-tpcds/dp_1T2c26g.json .
```

Once you got access to the file, run this [python program](https://github.com/DigitalWNZ/gcp_python_code/blob/main/parse_tpcds_result.py) to parse the json data and ingested into Bigquery. \
Sample Query to view total execution time of each iteration:
```
with base as (
  SELECT 
    iteration,
    sum(executionTime)/60000 as totalExecutionTime,
    sum(parsingTime + analysisTime + optimizationTime + planningTime + executionTime)/60000 as totalTime
  FROM `agolis-allen-first.IGG.dp_tpcds_v1` 
  group by 1
)
select 
  exp(sum(log(totalExecutionTime)) / count(*)) as geomean_executionTime,
  exp(sum(log(totalTime)) / count(*)) as geomean_totalTime
from base
```