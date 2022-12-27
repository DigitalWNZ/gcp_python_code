from google.cloud import bigquery_datatransfer
import os

# The project where the query job runs is the same as the project
# containing the destination dataset.
project_id = "agolis-allen-first"
dataset_id = "ELM"

path_to_credential = '/Users/wangez/Downloads/agolis-allen-first-2a651eae4ca4.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential

transfer_client = bigquery_datatransfer.DataTransferServiceClient()

# This service account will be used to execute the scheduled queries. Omit
# this request parameter to run the query as the user with the credentials
# associated with this client.
# service_account_name = "abcdef-test-sa@abcdef-test.iam.gserviceaccount.com"

# Use standard SQL syntax for the query.
query_string = """

Create or replace table ELM.schedule_test as SELECT
  CURRENT_TIMESTAMP() as current_time,
  @run_time as intended_run_time,
  @run_date as intended_run_date,
  17 as some_integer;

"""

parent = transfer_client.common_location_path(project_id,location="US")


transfer_config = bigquery_datatransfer.TransferConfig(
    display_name="Your Scheduled Query Name",
    data_source_id="scheduled_query",
    params={
        "query": query_string,
    },
    schedule="every 24 hours",
)

transfer_config = transfer_client.create_transfer_config(
    bigquery_datatransfer.CreateTransferConfigRequest(
        parent=parent,
        transfer_config=transfer_config,
    )
)

print("Created scheduled query '{}'".format(transfer_config.name))