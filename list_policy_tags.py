from google.cloud import bigquery
from google.iam.v1 import iam_policy_pb2,policy_pb2
from google.cloud import datacatalog_v1
from google.cloud.bigquery import datapolicies_v1
import os

if __name__ == '__main__':
    path_to_credential = '/Users/wangez/Downloads/agolis-allen-first-2a651eae4ca4.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential

    dc_client=datacatalog_v1.PolicyTagManagerClient()
    dc_client.list_policy_tags(parent='')
