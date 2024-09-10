from google.cloud import bigquery
import time
import os

if __name__ == '__main__':

    path_to_credential = '/Users/wangez/Downloads/GCP_Credentials/agolis-allen-first-13f3be86c3d1.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential

    project='agolis-allen-first'
    dataset = 'personal_data'
    table='streaming_test'
    full_result_table='{}.{}.{}'.format(project,dataset,table)
    client = bigquery.Client(project)

    create_table_sql = 'create table if not exists `{}` (' \
                       'int_value int64,' \
                       'string_value string)'.format(full_result_table)

    create_table_job=client.query(create_table_sql)
    #wait till the job done
    create_table_result=create_table_job.result()


    num_insert=2;
    for i in range(1,num_insert):
        print('insert rec {}'.format(str(i)))

        list_rec  = []
        rec={}
        rec['int_value']=i
        rec['string_value']='string_value_{}'.format(str(i))
        list_rec.append(rec)
        client.insert_rows_json(full_result_table,list_rec)

        time.sleep(3)

    print("job completed")






