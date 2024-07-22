import os
import json
import time
from google.cloud import bigquery

if __name__ == '__main__':
    path_to_credential = '/Users/wangez/Downloads/GCP_Credentials/agolis-allen-first-13f3be86c3d1.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential

    print('Create table if not exist')
    project='agolis-allen-first'
    table='agolis-allen-first.IGG.emr_dp_tpcds_v1'
    client = bigquery.Client(project=project)
    create_table_sql = 'create table if not exists `{}` (' \
                       'ts int64,' \
                       'iteration string,' \
                       'name string,' \
                       'parsingTime float64,' \
                       'analysisTime float64,' \
                       'optimizationTime float64,' \
                       'planningTime float64,' \
                       'executionTime float64)'.format(table)
    create_table_job=client.query(create_table_sql)
    create_table_result=create_table_job.result()
    time.sleep(90)

    print('Parse json result')
    rows=[]
    json_file=open('/Users/wangez/Downloads/part-00000-737b5867-1017-4ee0-8d8f-2fecb15f9558-c000.json','r')
    for line in json_file:
        dict_line=json.loads(line)
        results=dict_line['results']
        for rec in results:
            row={}
            row['ts']=dict_line['timestamp']
            row['iteration'] = 'iteration_{}'.format(dict_line['iteration'])
            row['name'] = rec['name']
            row['parsingTime'] = rec['parsingTime']
            row['analysisTime'] = rec['analysisTime']
            row['optimizationTime'] = rec['optimizationTime']
            row['planningTime'] = rec['planningTime']
            row['executionTime'] = rec['executionTime']
            rows.append(row)

    print(rows)
    client.insert_rows_json(table, rows)

