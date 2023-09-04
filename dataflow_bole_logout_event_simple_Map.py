import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms import window
from google.cloud import bigquery
import os
import logging,json
from typing import Tuple,Iterable,Dict
from apache_beam.io.gcp.bigquery_tools import RetryStrategy


def run(argv=None,save_main_session=True):
    parser=argparse.ArgumentParser()
    parser.add_argument('--outputTable',
                       dest='outputTable',
                       required=True)
    parser.add_argument('--stagingLocation',
                       dest='stagingLocation',
                       required=True)
    parser.add_argument('--tempLocation',
                       dest='tempLocation',
                       required=True)
    parser.add_argument('--runner',
                       dest='runner',
                       required=True)

    group=parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--inputTopic',
                       dest='inputTopic')
    group.add_argument('--inputSub',
                       dest='inputSub')

    known_args,pipeline_args=parser.parse_known_args(argv)
    pipeline_options=PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session=save_main_session
    pipeline_options.view_as(StandardOptions).streaming=True


    p=beam.Pipeline(runner=known_args.runner,options=pipeline_options)
    if known_args.inputSub:
       message=(
            p|beam.io.ReadFromPubSub(subscription=known_args.inputSub,with_attributes=True))
    else:
       message=(
           p|beam.io.ReadFromPubSub(topic=known_args.inputTopic,with_attributes=True))

    def extract_element_Fn(element)->Tuple[str,Dict]:
        print("extractElement Start")
        data = element.data.decode('utf-8')
        if json.loads(data).get('event_name') == 'logout':
            user_id = json.loads(data).get('user_id')
            """https://stackoverflow.com/questions/53912918/difference-between-beam-pardo-and-beam-map-in-the-output-type
            Best practice: output list in ParDo(),single object in Map
            """
            return (user_id,data)

    mainData=(
        message
        |'filter logout event'>>beam.Map(extract_element_Fn)
        |'window' >> beam.WindowInto(window.FixedWindows(5,0))
        |'group by key' >> beam.GroupByKey()
    )

    def enrich_country_Fn(element)->Tuple[str,str]:
        print("Enrich Country Start")
        user_id=element[0]
        query = 'select country from `agolis-allen-first.dataflow_bole.country_dim` where user_id="{}"' \
           .format(user_id)
        client=bigquery.Client()
        query_job = client.query(query)
        result=query_job.result()

        status=None
        country=None
        len_result = 0
        for row in result:
            country=row.country
            len_result+=1

        if len_result == 0:
            status=OUTPUT_TAG_NO_REC
        else:
            status = OUTPUT_TAG_COMPLETE

        return (user_id,country)

    enrichCountry = (
        mainData
        |'enrich country via ParDo' >> beam.Map(enrich_country_Fn)
    )

    def enrich_history_Fn(element):
            print("Enrich History Start")
            user_id=element[0]
            query = 'select event_date,event_name,device from `agolis-allen-first.dataflow_bole.event_history` where user_id="{}"' \
               .format(user_id)
            client=bigquery.Client()
            query_job = client.query(query)
            result=query_job.result()

            status=None
            event_params=[]

            len_result = 0
            for row in result:
                single_event_params={}
                single_event_params['event_date']=row.event_date
                single_event_params['event_name'] = row.event_name
                single_event_params['device'] = row.device
                event_params.append(single_event_params)
                len_result+=1

            if len_result == 0:
                status=OUTPUT_TAG_NO_REC
            else:
                status = OUTPUT_TAG_COMPLETE

            return (user_id,event_params)

    enrichHistory = (
        mainData |'enrich history' >> beam.Map(enrich_history_Fn)
    )

    def merge_data(element):
        print("Merge Data Start")
        result_json={}
        result_json["user_id"]=element[0]
        result_json["country"]=element[1][0][0]
        result_json["events"]=element[1][1][0]
        return result_json

    processedData = (
        (enrichCountry,enrichHistory)
        |beam.CoGroupByKey()
        |'combine data' >> beam.Map(merge_data)
        |'write complete data to bq' >> beam.io.WriteToBigQuery(
        table='agolis-allen-first:dataflow_bole.result',
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR
        )
    )

    p.run().wait_until_finish()

if __name__ == '__main__':
    path_to_credential = '/Users/wangez/Downloads/GCP_Credentials/agolis-allen-first-13f3be86c3d1.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential
    logging.getLogger().setLevel(logging.INFO)

    OUTPUT_TAG_NO_REC = 'Norecord'
    OUTPUT_TAG_COMPLETE = 'complete'
    OUTPUT_TAG_FAILURE = 'failure'

    run()
