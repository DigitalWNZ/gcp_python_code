from google.cloud import bigquery
import os

def query_test(sql):
    client=bigquery.Client()
    query_job=client.query(sql)
    return query_job.result()

def role_test ():
    client=bigquery.Client()
    ds=client.get_dataset('ELM')



if __name__ == '__main__':

    path_to_credential = '/Users/wangez/Downloads/agolis-allen-first-2a651eae4ca4.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential
    sql='insert into `agolis-allen-first.ELM.a` values(1,"2023-01-01")'
    res=query_test(sql)
    print(res)
    # client = bigquery.Client()
    # table=client.get_table('logsink.cloudaudit_googleapis_com_data_access')
    # attr=client.get_iam_policy(table=table)
    # ds = client.get_dataset('ELM')
    print('hi')