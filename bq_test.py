from google.cloud import bigquery
import os

def query_test(sql):
    client=bigquery.Client()
    query_job=client.query(sql)
    return query_job.result()

def role_test ():
    client=bigquery.Client()
    ds=client.get_dataset('ELM')

def update_billing_model ():
    client=bigquery.Client()
    dataset_id = 'agolis-allen-first.ELM'
    dataset = client.get_dataset(dataset_id)  # Make an API request.

    # Set the default partition expiration (applies to new tables, only) in
    # milliseconds. This example sets the default expiration to 90 days.
    dataset.default_partition_expiration_ms = 90 * 24 * 60 * 60 * 1000

    dataset = client.update_dataset(
        dataset, ["default_partition_expiration_ms"]
    )  # Make an API request.


if __name__ == '__main__':

    path_to_credential = '/Users/wangez/Downloads/GCP_Credentials/agolis-allen-first-13f3be86c3d1.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential
    sql='insert into `agolis-allen-first.ELM.a` values(1,"2023-01-01")'
    res=query_test(sql)
    print(res)
    # client = bigquery.Client()
    # table=client.get_table('logsink.cloudaudit_googleapis_com_data_access')
    # attr=client.get_iam_policy(table=table)
    # ds = client.get_dataset('ELM')
    print('hi')