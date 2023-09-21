from google.cloud import bigquery
import time
import os

if __name__ == '__main__':

    path_to_credential = '/Users/wangez/Downloads/GCP_Credentials/agolis-allen-first-13f3be86c3d1.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential

    default_project='agolis-allen-first'
    default_dataset='tpcds_data_320'

    result_project='agolis-allen-first'
    result_dataset='tpcds_data_320'
    result_table='tpcds_result'
    full_result_table='{}.{}.{}'.format(result_project,result_dataset,result_table)

    query_category='Bigquery Native'
    query_path='./generated_query_320/{}.sql'
    query_run_times=1
    client=bigquery.Client(default_project)
    job_config=bigquery.QueryJobConfig(use_query_cache=False,dry_run=True,default_dataset='{}.{}'.format(default_project,default_dataset))
    for i in range(1,100):
        print('Run job {}'.format(str(i)))
        f=open(query_path.format(str(i)))
        sql=f.read()

        resp_query_run_rec=[]
        for x in range(0,query_run_times):
            print('Run job {} for the {} time'.format(str(i),str(x+1)))
            job=client.query(sql,job_config=job_config)
            start_time = time.time()
            job_id=job.job_id
            res=job.result()
            end_time=time.time()
            duration = int((end_time - start_time) * 1000)

            rec={}
            rec['category']=query_category
            rec['sn']=i
            rec['run_sn']=x
            rec['job_id']=job_id
            rec['start_time']=start_time
            rec['end_time']=end_time
            rec['duration']=duration
            resp_query_run_rec.append(rec)

        # client.insert_rows_json(full_result_table,resp_query_run_rec)

    print("job completed")






