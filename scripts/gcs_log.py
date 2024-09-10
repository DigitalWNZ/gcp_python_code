import sys
from google.cloud import storage
import numpy as np

timestamp = sys.argv[1]
cluster_name = sys.argv[2]

def get_numbers_from_gcs(bucket_name, blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    try:
        content = blob.download_as_text()
        numbers = [int(x) for x in content.split() if x.isdigit()]
        return numbers
    except Exception as e:  # Broad exception handling for demonstration
        print(f"Error reading/processing file from GCS: {e}")
        return None

bucket_name = "shein-hbase-anywhere-cache-test"
all_numbers = []
for i in range(0, 7):
        blob_name = f"ycsb_hbase_result/gcs_log/{cluster_name}-w-{i}-{timestamp}.txt"
        numbers = get_numbers_from_gcs(bucket_name, blob_name)
        print(f"file: {blob_name} length:{len(numbers)}")
        all_numbers += numbers

n = np.array(all_numbers)
percentile_99 = round(np.percentile(numbers, 99))
percentile_999 = round(np.percentile(numbers, 99.9))
percentile_9999 = round(np.percentile(numbers, 99.99))

print(f"percentile_99: {percentile_99} ns, percentile_999: {percentile_999} ns, percentile_9999: {percentile_9999} ns")
