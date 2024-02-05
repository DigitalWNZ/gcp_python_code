from google.cloud import bigquery
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types
from google.cloud.bigquery_storage_v1 import writer
from google.protobuf import descriptor_pb2
import json
import os
import logging
import app_access_row_pb2
from time import sleep

def create_row_data(row_json):
    row = app_access_row_pb2.CustomerRecord()
    user_name=list(row_json.keys())[0]
    body=row_json[user_name]
    #If the incoming field is null,such as body['target'] = None, you can not simply extend.
    # because BQ will deem this as missing field
    if user_name is None:
        row.user_name = ''
    else:
        row.user_name=user_name

    if body['target'] is None:
        row.target.extend([])
    else:
        row.target.extend(body['target'])

    if body['authLevel'] is None:
        row.target.extend([])
    else:
        row.authLevel.extend(body['authLevel'])

    if body['geolocation'] is None:
        row.target.extend([])
    else:
        row.geolocation.extend(body['geolocation'])

    if body['userAgent'] is None:
        row.target.extend([])
    else:
        row.userAgent.extend(body['userAgent'])

    if body['appflag'] is None:
        row.target.extend([])
    else:
        row.appflag.extend(body['appflag'])

    if body['ip'] is None:
        row.target.extend([])
    else:
        row.ip.extend(body['ip'])

    if body['hitDictLevel'] is None:
        row.hitDictLevel = ''
    else:
        row.hitDictLevel= body['hitDictLevel']

    # The following line are for test purpose only
    # row.target.extend(['t'])
    # row.authLevel.extend([5])
    # row.geolocation.extend(['g'])
    # row.userAgent.extend(['ua'])
    # row.appflag.extend(['a'])
    # row.ip.extend(['1.1.1.1'])
    # row.hitDictLevel='x'

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

    # client=bigquery.Client()
    # schema=[
    #     bigquery.SchemaField("user_name","string",mode="REQUIRED"),
    #     bigquery.SchemaField("target", "string", mode="REPEATED"),
    #     bigquery.SchemaField("authLevel", "int64", mode="REPEATED"),
    #     bigquery.SchemaField("geolocation", "string", mode="REPEATED"),
    #     bigquery.SchemaField("userAgent", "string", mode="REPEATED"),
    #     bigquery.SchemaField("appflag", "string", mode="REPEATED"),
    #     bigquery.SchemaField("ip", "string", mode="REPEATED"),
    #     bigquery.SchemaField("hitDictLevel", "string", mode="REQUIRED")
    # ]
    #
    # table = bigquery.Table(table_ref='agolis-allen-first.experiment.app_access',schema=schema)
    # client.delete_table(table=table,not_found_ok=True)
    # table=client.create_table(table=table,exists_ok=True)



    write_client=bigquery_storage_v1.BigQueryWriteClient()
    parent=write_client.table_path('agolis-allen-first','experiment','app_access')
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
    app_access_row_pb2.CustomerRecord.DESCRIPTOR.CopyToProto(proto_descriptor)
    proto_schema.proto_descriptor=proto_descriptor
    proto_data=types.AppendRowsRequest.ProtoData()
    proto_data.writer_schema=proto_schema
    request_template.proto_rows=proto_data

    append_rows_stream=writer.AppendRowsStream(write_client,request_template)

    f_log=open('/Users/wangez/Downloads/zl_app_access_log.txt','r')
    lines=f_log.readlines()
    offset=0
    for line in lines:
        # The logic below is for test purpose only
        # if offset > 10:
        #     break
        line_json=json.loads(line)
        row=create_row_data(line_json)
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





