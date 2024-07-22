# Spark TPC-DS on Bigquery
## Create Reservation (0-100) in us-central1 
## Update [query execution orchestration program](https://github.com/DigitalWNZ/gcp_python_code/blob/main/tpcds_query_execution.py)
Change the following variables in the orchestration program
- default_project: The project to store data is used as default project to run queries. 
- default_dateset: The dataset contains TPCDS Data
- {result_project}.{result_dataset}.{result_table} - Destination table to store the benchmark result
- {result_project}.{result_dataset}.{cross_result_table} - Destination table to store the benchmark result along with data from INFORMATION_SCHEMA
- run_id: Identify one benchmark execution
- query_category: Description of the benchmark execution
- query_run_times: Number of times each individual query will be executed 
- dry_run_flag: Whether the queries will be executed in dry-run mode,mainly to validate queries. 

## Run query execution orchestration program
## Check benchmark result
Go to Biquery to run queries to analyze the result, sample queries: 
```
with base as (
  select 
    run_id,
    category,
    run_sn,
    round(sum(job_duration)/60000,2) as job_duration,
    sum(total_slot_ms) as total_slot_ms,
    avg(avg_slots) as avg_slots,
  from  `agolis-allen-first.tpcds_data_320_1T_v1_cluster.tpcds_cross_result`
  group by 1,2,3
)
select
  -- *
  round(exp(sum(log(job_duration)) / count(*)),2) as job_duration,
  round(exp(sum(log(total_slot_ms)) / count(*)),2) as total_slot_ms,
  round(avg(avg_slots),2) as avg_avg_slots,
from base
```