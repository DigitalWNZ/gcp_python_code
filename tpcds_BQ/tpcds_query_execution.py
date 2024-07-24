from google.cloud import bigquery
import time
import os

if __name__ == '__main__':

    path_to_credential = '/Users/wangez/Downloads/GCP_Credentials/agolis-allen-first-13f3be86c3d1.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential

    default_project='agolis-allen-first'

    # default_dataset = 'tpcds_data_320_1T_v1'
    default_dataset = 'tpcds_data_320_1T_v1_cluster'
    # default_dataset = 'tpcds_data_320_1T_iceberg_BLMT_v1'
    region='us-central1'

    result_project='agolis-allen-first'

    # result_dataset = 'tpcds_data_320_1T_v1'
    result_dataset = 'tpcds_data_320_1T_v1_cluster'
    # result_dataset='tpcds_data_320_1T_iceberg_BLMT_v1'

    result_table='tpcds_result_20240722'
    cross_result_table='tpcds_cross_result_20240722'
    # result_table='tpcds_result_cmeta'
    # cross_result_table='tpcds_cross_result_cmeta'

    full_result_table='{}.{}.{}'.format(result_project,result_dataset,result_table)
    full_cross_result_table = '{}.{}.{}'.format(result_project, result_dataset, cross_result_table)


    run_id='20270722'
    query_category = 'Bigquery TPCDS for IGG'

    query_path= 'generated_query_320/{}.sql'
    query_run_times=1
    dry_run_flag=True
    client=bigquery.Client(default_project)

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

    job_config=bigquery.QueryJobConfig(use_query_cache=False,dry_run=dry_run_flag,default_dataset='{}.{}'.format(default_project,default_dataset))
    resp_query_run_rec = []
    st_time= time.time()
    print(st_time)
    for i in range(1,100):
        print('Run job {}'.format(str(i)))
        f=open(query_path.format(str(i)))
        sql=f.read()

        # resp_query_run_rec=[]
        for x in range(0,query_run_times):
            print('Run job {} for the {} time'.format(str(i),str(x+1)))
            job=client.query(sql,job_config=job_config)
            start_time = time.time()
            job_id=job.job_id
            res=job.result()
            end_time=time.time()
            duration = int((end_time - start_time) * 1000)

            rec={}
            rec['run_id']=run_id
            rec['category']=query_category
            rec['sn']=i
            rec['run_sn']=x
            rec['job_id']=job_id
            rec['client_start_time']=start_time
            rec['client_end_time']=end_time
            rec['client_duration']=duration
            resp_query_run_rec.append(rec)

    for i in ['14b','23b','24b','39b']:
        print('Run job {}'.format(str(i)))
        f=open(query_path.format(i))
        sql=f.read()

        # resp_query_run_rec=[]
        for x in range(0,query_run_times):
            print('Run job {} for the {} time'.format(str(i),str(x+1)))
            job=client.query(sql,job_config=job_config)
            start_time = time.time()
            job_id=job.job_id
            res=job.result()
            end_time=time.time()
            duration = int((end_time - start_time) * 1000)

            rec={}
            rec['run_id']=run_id
            rec['category']=query_category
            rec['sn']=i
            rec['run_sn']=x
            rec['job_id']=job_id
            rec['client_start_time']=start_time
            rec['client_end_time']=end_time
            rec['client_duration']=duration
            resp_query_run_rec.append(rec)
    ed_time=time.time()
    print(ed_time)
    job_dur=ed_time - st_time
    print("job elapse {} seconds".format(str(job_dur)))

    if not dry_run_flag:
        client.insert_rows_json(full_result_table,resp_query_run_rec)
        job_stat_sql = 'create or replace table `{}` as select \n' \
                       'a.run_id,a.category,a.sn,a.run_sn,a.job_id,a.client_start_time,a.client_end_time,a.client_duration,\n' \
                       'b.creation_time,b.start_time,b.end_time,TIMESTAMP_DIFF(b.end_time, b.start_time, MILLISECOND) AS job_duration,\n' \
                       'b.total_bytes_processed, b.total_bytes_billed,b.total_slot_ms,\n' \
                       'b.total_slot_ms / (TIMESTAMP_DIFF(b.end_time, b.start_time, MILLISECOND)) AS avg_slots \n' \
                       'from `{}` a \n' \
                       'inner join {}.`region-{}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT b \n' \
                       'on a.job_id = b.job_id \n' \
                       'where a.run_id="{}"'.format(full_cross_result_table,full_result_table,default_project,region,run_id)
        stat_job=client.query(job_stat_sql)
        #wait till the job done
        stat_result=stat_job.result()

    print("job completed")






