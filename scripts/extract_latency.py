import re
import os

def extract_ycsb_metrics(file_path):
    metrics = {}
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            if "[OVERALL], Throughput(ops/sec)," in line:
                metrics["Overall Throughput (ops/sec)"] = float(line.split(",")[2].strip())
            elif "[READ], 99thPercentileLatency(us)," in line:
                metrics["Read 99thPercentileLatency (us)"] = int(line.split(",")[2].strip())
            elif "[READ], 99.9PercentileLatency(us)," in line:
                metrics["Read 99.9PercentileLatency (us)"] = int(line.split(",")[2].strip())
            elif "[READ], 99.99PercentileLatency(us)," in line:
                metrics["Read 99.99PercentileLatency (us)"] = int(line.split(",")[2].strip())
    return metrics

def process_directory(directory_path):
    sorted_files = sorted([f for f in os.listdir(directory_path)])
    for filename in sorted_files:
        file_path = os.path.join(directory_path, filename)
        extracted_metrics = extract_ycsb_metrics(file_path)
        if extracted_metrics:
            values = []
            for metric, value in extracted_metrics.items():
                values.append(round(float(value)) if 'Throughput' in metric else round(float(value)/1000))
            print(f"File: {filename} values: {values}")

process_directory("/Users/mengjiaowang/Downloads/log")