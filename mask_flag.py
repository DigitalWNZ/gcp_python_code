from google.cloud import bigquery
import os

path_to_credential = '/Users/wangez/Downloads/agolis-allen-first-2a651eae4ca4.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential
# Construct a BigQuery client object.
client = bigquery.Client()


query='SELECT ccore  as c FROM `agolis-allen-first.ELM.cpu_model` LIMIT 1'
query_job = client.query(query)  # Make an API request.

if query_job._properties['statistics'].__contains__('dataMaskingStatistics') == True:
    print("mask is applied")
else:
    print("mask is not applied")

query='SELECT acore  as c FROM `agolis-allen-first.ELM.cpu_model` LIMIT 1'

query_job = client.query(query)  # Make an API request.
if query_job._properties['statistics'].__contains__('dataMaskingStatistics') == True:
    print("mask is applied")
else:
    print("mask is not applied")

print("The query data:")
for row in query_job:
    # Row values can be accessed by field name or index.
    print("name={}, count={}".format(row[0], row[1]))