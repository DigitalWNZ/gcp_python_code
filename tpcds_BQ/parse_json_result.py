import os
import json
import time
from google.cloud import bigquery

if __name__ == '__main__':
    path_to_credential = '/Users/wangez/Downloads/GCP_Credentials/agolis-allen-first-13f3be86c3d1.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential


    result_project='agolis-allen-first'
    region='us_central1'
    result_dataset = 'IGG'
    result_table='tpcds_result_20240725'
    cross_result_table='tpcds_cross_result_20240725'
    full_result_table='{}.{}.{}'.format(result_project,result_dataset,result_table)
    full_cross_result_table = '{}.{}.{}'.format(result_project, result_dataset, cross_result_table)
    run_id='20240725'
    client = bigquery.Client(result_project)

    create_table_sql = 'create table if not exists `{}` (' \
                       'run_id string,' \
                       'category string,' \
                       'sn int64,' \
                       'run_sn int64,' \
                       'job_id string,' \
                       'client_start_time timestamp,' \
                       'client_end_time timestamp,' \
                       'client_duration int64)'.format(full_result_table)
    create_table_job=client.query(create_table_sql)
    #wait till the job done
    create_table_result=create_table_job.result()


    print('Parse json result')
    rows=[]
    json_file=open('/Users/wangez/Downloads/run_data_20240725.txt','r')
    for line in json_file:
        dict_line=json.loads(line.replace("'","\""))
        rows.append(dict_line)

    print(rows)
    client.insert_rows_json(full_result_table, rows)

    job_stat_sql = 'create or replace table `{}` as select \n' \
                   'a.run_id,a.category,a.sn,a.run_sn,a.job_id,a.client_start_time,a.client_end_time,a.client_duration,\n' \
                   'b.creation_time,b.start_time,b.end_time,TIMESTAMP_DIFF(b.end_time, b.start_time, MILLISECOND) AS job_duration,\n' \
                   'b.total_bytes_processed, b.total_bytes_billed,b.total_slot_ms,\n' \
                   'b.total_slot_ms / (TIMESTAMP_DIFF(b.end_time, b.start_time, MILLISECOND)) AS avg_slots \n' \
                   'from `{}` a \n' \
                   'inner join {}.`region-{}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT b \n' \
                   'on a.job_id = b.job_id \n' \
                   'where a.run_id="{}"'.format(full_cross_result_table, full_result_table, result_project, region,
                                                run_id)
    stat_job = client.query(job_stat_sql)
    # wait till the job done
    stat_result = stat_job.result()

