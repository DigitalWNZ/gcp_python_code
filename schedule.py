import os

from google.cloud import bigquery,storage,bigquery_datatransfer
from google.oauth2 import service_account



if __name__ == '__main__':
    # Value to be changed
    # name= 'events_20200901'
    # table_pattern = '^events_'+ '[0-9]{8}$'
    # pattern = re.compile(table_pattern, re.IGNORECASE)
    # found = pattern.findall(name)

    path_to_credential = '/Users/wangez/Desktop/global-apm-45f3acc33771.json'
    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential

    credentials = service_account.Credentials.from_service_account_file(
        path_to_credential, scopes=["https://www.googleapis.com/auth/cloud-platform"],)
    transfer_client = bigquery_datatransfer.DataTransferServiceClient(credentials=credentials)

    # The project where the query job runs is the same as the project
    # containing the destination dataset.
    project_id = "global-apm"
    dataset_id = "howl_preview_41"

    # This service account will be used to execute the scheduled queries. Omit
    # this request parameter to run the query as the user with the credentials
    # associated with this client.
    service_account_name = "global-apm@appspot.gserviceaccount.com"

    # Use standard SQL syntax for the query.
    query_string = """
    SELECT
      CURRENT_TIMESTAMP() as current_time,
      @run_time as intended_run_time,
      @run_date as intended_run_date,
      17 as some_integer
    """

    parent = transfer_client.common_project_path(project_id)

    transfer_config = bigquery_datatransfer.TransferConfig(
        destination_dataset_id=dataset_id,
        display_name="Your Scheduled Query Name",
        data_source_id="scheduled_query",
        params={
            "query": query_string,
            "destination_table_name_template": "your_table_{run_date}",
            "write_disposition": "WRITE_TRUNCATE",
            "partitioning_field": "",
        },
        schedule="every 24 hours",
    )

    transfer_config = transfer_client.create_transfer_config(
        bigquery_datatransfer.CreateTransferConfigRequest(
            parent=parent,
            transfer_config=transfer_config,
            service_account_name=service_account_name,
        )
    )

    print("Created scheduled query '{}'".format(transfer_config.name))


