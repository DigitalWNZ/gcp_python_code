from google.cloud import bigquery
import os

if __name__ == '__main__':

    path_to_credential = '/Users/wangez/Downloads/GCP_Credentials/agolis-allen-first-2a651eae4ca4.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential

    client = bigquery.Client()

    query = """
        SELECT states
        FROM UNNEST(@states) states;
    """
    var_struct = bigquery.StructQueryParameter(
        "struct_value",
        bigquery.ScalarQueryParameter("x", "INT64", 1),
        bigquery.ScalarQueryParameter("y", "STRING", "foo"),
    )

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("states", "STRUCT", [var_struct,var_struct]),
        ]
    )
    query_job = client.query(query, job_config=job_config)  # Make an API request.

    for row in query_job:
        print(row)

