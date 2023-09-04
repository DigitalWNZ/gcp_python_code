from google.cloud import bigquery_datatransfer
from google.protobuf import field_mask_pb2

transfer_client = bigquery_datatransfer.DataTransferServiceClient()

service_account_name = "abcdef-test-sa@abcdef-test.iam.gserviceaccount.com"
transfer_config_name = "projects/1234/locations/us/transferConfigs/abcd"

transfer_config = bigquery_datatransfer.TransferConfig(name=transfer_config_name)

transfer_config = transfer_client.update_transfer_config(
    {
        "transfer_config": transfer_config,
        "update_mask": field_mask_pb2.FieldMask(paths=["service_account_name"]),
        "service_account_name": service_account_name,
    }
)

print("Updated config: '{}'".format(transfer_config.name))