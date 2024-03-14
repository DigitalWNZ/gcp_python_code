from google.cloud import bigquery
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types
from google.cloud.bigquery_storage_v1 import writer
from google.protobuf import descriptor_pb2
import json
import os
import logging
import tunnelblock_row_pb2
import glob
import re
from time import sleep

def create_row_data(row_json,dt):
    row = tunnelblock_row_pb2.tunnelBlockMsg()
    user_name=list(row_json.keys())[0]
    body=row_json[user_name]
    #If the incoming field is null,such as body['target'] = None, you can not simply extend.
    # because BQ will deem this as missing field
    if user_name is None:
        row.userkey = ''
    else:
        row.userkey=user_name
    row.dt=dt

    for item in body:
        element= row.rec.add()
        element.ip=item['ip']
        element.target=item['target']
        element.username=item['username']

    return row.SerializeToString()



if __name__ == '__main__':

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler()
        ]
    )

    path_to_credential = '/Users/wangez/Downloads/GCP_Credentials/agolis-allen-first-13f3be86c3d1.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential

    client=bigquery.Client()
    # schema=[
    #     bigquery.SchemaField("userkey","string",mode="REQUIRED"),
    #     bigquery.SchemaField("rec", "record", mode="REPEATED",
    #                          fields=[
    #                              bigquery.SchemaField("ip", "string", mode="nullable"),
    #                              bigquery.SchemaField("target", "string", mode="nullable"),
    #                              bigquery.SchemaField("username", "string", mode="nullable")
    #                          ]),
    #     bigquery.SchemaField("dt", "string", mode="nullable")
    # ]
    #
    # table = bigquery.Table(table_ref='agolis-allen-first.experiment.tunnelblock',schema=schema)
    # client.delete_table(table=table,not_found_ok=True)
    # table=client.create_table(table=table,exists_ok=True)
    # exit(0)

    write_client=bigquery_storage_v1.BigQueryWriteClient()
    parent=write_client.table_path('agolis-allen-first','experiment','tunnelblock')
    write_stream=types.WriteStream()

    write_stream.type_=types.WriteStream.Type.PENDING
    write_stream=write_client.create_write_stream(
        parent=parent,write_stream=write_stream
    )
    stream_name=write_stream.name

    request_template=types.AppendRowsRequest()
    request_template.write_stream=stream_name

    proto_schema=types.ProtoSchema()
    proto_descriptor=descriptor_pb2.DescriptorProto()
    tunnelblock_row_pb2.tunnelBlockMsg.DESCRIPTOR.CopyToProto(proto_descriptor)
    proto_schema.proto_descriptor=proto_descriptor
    proto_data=types.AppendRowsRequest.ProtoData()
    proto_data.writer_schema=proto_schema
    request_template.proto_rows=proto_data

    append_rows_stream=writer.AppendRowsStream(write_client,request_template)

    path='/Users/wangez/Desktop/app_access_log/tunnelblock_*.txt'
    offset=0
    for filename in glob.glob(path):
        dt=re.findall(r"tunnelblock_(.*?).txt", filename)[0]
        f_log=open(filename,'r')
        lines=f_log.readlines()
        for line in lines:
            # The logic below is for test purpose only
            # if offset > 10:
            #     break
            line_json=json.loads(line)
            row=create_row_data(line_json,dt)
            proto_rows = types.ProtoRows()
            proto_rows.serialized_rows.append(row)
            request=types.AppendRowsRequest()
            print(str(offset))
            request.offset=offset
            offset+=1
            proto_data=types.AppendRowsRequest.ProtoData()
            proto_data.rows=proto_rows
            request.proto_rows=proto_data
            request_future=append_rows_stream.send(request)
            print(request_future.result())

    append_rows_stream.close()
    write_client.finalize_write_stream(name=write_stream.name)

    batch_commit_write_streams_request=types.BatchCommitWriteStreamsRequest()
    batch_commit_write_streams_request.parent=parent
    batch_commit_write_streams_request.write_streams=[write_stream.name]
    write_client.batch_commit_write_streams(batch_commit_write_streams_request)

    print('done')





