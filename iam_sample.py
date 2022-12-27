from google.cloud import bigquery

import os

if __name__ == '__main__':
    path_to_credential = '/Users/wangez/Downloads/agolis-allen-first-2a651eae4ca4.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential
    client=bigquery.Client()

    table=client.get_table('agolis-allen-first.ELM.all_dt_test')
    role='roles/bigquery.dataViewer'



    p = client.get_iam_policy(table)
    user="user:wangez@google.com"
    p.bindings.append({"role":"roles/bigquery.dataViewer","members":{user}})
    client.set_iam_policy(table,p)


    print('x')



