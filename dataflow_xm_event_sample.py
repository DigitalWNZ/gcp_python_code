# ********************
#  input
#  {"user_id":1, "cpc":0.15, "region":"US"}

#  1 - Check data exist in hbase or not
#   1.1 - If exist and status = done , ignore the following processing
#   1.2 - If exist and status !=done, go to next step
#   1.3 - If not exist, store the data in hbase
#  2 - normal processing and store data in bigquery
#      2.1 - In case of error, resend the message back to pubsub (same of different topic)
#  3 - set the hbase status to done
#
#
#  In case of uncatched exception, a monitor program will drain the pipeline and restart the pipeline.
# ********************
import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from google.cloud import bigquery
import apache_beam.io.gcp.pubsub as pubsub
# import apache_beam.io.gcp.bigquery as bigquery
import re,os
import logging,json
from apache_beam.io.gcp.bigquery_tools import RetryStrategy

class extractElement(beam.DoFn):
   def process(self, element, *args, **kwargs):
       try:
           print("extractElement Start")
           attribute = element.attributes
           data = element.data.decode('utf-8')
           if attribute.get('uid') is None:
               raise ValueError("No uid is provided in the message")
           # A process method of a DoFn should return an Iterable[T] to produce a PCollection[T],
           # so it should be declared as returning -> Iterable[Tuple[str, str]] other than tuple(str,str)
           # Otherwise, only the second item will be returned.
           yield (data,attribute)
       except Exception as err:
           step_name = 'extractElement'
           failure = (data,attribute,step_name)
           yield beam.pvalue.TaggedOutput(OUTPUT_TAG_FAILURE,failure)

class enrichByBQClient(beam.DoFn):

    def process(self, element, *args, **kwargs):
        try:
            print("Enrich from BQ Start")
            attribute = element[1]
            if attribute.get('uid') is None:
                raise ValueError("No uid is provided in the message")
            query = 'select uid,status from `agolis-allen-first.experiment.dataflow_enrich` where uid="{}" limit 1' \
               .format(attribute.get('uid'))
            client=bigquery.Client()
            query_job = client.query(query)
            result=query_job.result()

            status=None
            len_result = 0
            for row in result:
                status=row.status
                len_result+=1

            if len_result == 0:
                status=OUTPUT_TAG_NEW
            elif status != 'complete':
                status = OUTPUT_TAG_INCOMPLETE
            else:
                status = OUTPUT_TAG_COMPLETE

            yield (element[0],element[1],status)
        except Exception as err:
            step_name = 'enrichByBQClient'
            failure = [(element[0],element[1], step_name)]
            yield beam.pvalue.TaggedOutput(OUTPUT_TAG_FAILURE, failure)

class businessLogic(beam.DoFn):

    def process(self, element, *args, **kwargs):
        try:
            # print("business Logic Start")
            if element[2] == OUTPUT_TAG_NEW:
                print('Main business logic for new')
                res=(element[0],element[1],'complete')
                yield beam.pvalue.TaggedOutput(OUTPUT_TAG_NEW,res)
            elif element[2] == OUTPUT_TAG_INCOMPLETE:
                res = (element[0], element[1], 'complete')
                print('Main business logic for incomplete')
                yield beam.pvalue.TaggedOutput(OUTPUT_TAG_INCOMPLETE,res)
            else:
                print('Main business logic for complete')
                res= (element[0], element[1], 'complete')
                yield beam.pvalue.TaggedOutput(OUTPUT_TAG_COMPLETE,res)
        except Exception as err:
            step_name = 'businessLogic'
            failure = (element[0],element[1], step_name)
            yield beam.pvalue.TaggedOutput(OUTPUT_TAG_FAILURE, failure)

class updateRow(beam.DoFn):
   def process(self, element, *args, **kwargs):
       try:
           print("updateRow Start")
           attribute = element[1]
           query = 'update `agolis-allen-first.experiment.dataflow_enrich` set status = "complete" where uid="{}" ' \
               .format(attribute.get('uid'))
           client=bigquery.Client()
           query_job = client.query(query)
           result=query_job.result()
       except Exception as err:
           step_name = 'updateRow'
           failure = [(element[0], element[1], step_name)]
           yield beam.pvalue.TaggedOutput(OUTPUT_TAG_FAILURE, failure)

class format_result_for_bq(beam.DoFn):
   def process(self, element, *args, **kwargs):
       try:
           print('format result for bq')
           yield {
               'uid': element[1].get('uid'),
               'data': element[0],
               'status': element[2]
               # 'test':'test'
           }
       except Exception as err:
           step_name = 'format BQ result'
           failure = [(element[0], element[1], step_name)]
           yield beam.pvalue.TaggedOutput(OUTPUT_TAG_FAILURE, failure)




class parseWriteResult(beam.DoFn):
   def process(self, element, *args, **kwargs):
       #  The input is tuple(2)
       # ('agolis-allen-first:experiment.dataflow_duplicate', {'uid': '1', 'data': '{"user_id":1, "cpc":0.15, "region":"US"}', 'status': 'complete', 'test': 'test'})
       print('parse bigquery error row')
       attr = {}
       attr['uid'] = element[1].get('uid')
       msg = pubsub.PubsubMessage(
           data=element[1].get('data').encode(encoding='UTF-8'),
           attributes=attr
       )
       yield msg



class format_data_for_pb(beam.DoFn):
   def process(self, element, *args, **kwargs):
       # The input is tuple(3)
       # ('{"user_id":1, "cpc":0.15, "region":"US"}', {}, 'extractElement')
       print("format result for pb Start")
       msg=pubsub.PubsubMessage(
           data=element[0].encode(encoding='UTF-8'),
           attributes=element[1]
       )
       yield msg

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
    # parser.add_argument('--streaming',
    #                     dest='streaming',
    #                     required=True)

    group=parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--inputTopic',
                       dest='inputTopic')
    group.add_argument('--inputSub',
                       dest='inputSub')

    known_args,pipeline_args=parser.parse_known_args(argv)
    pipeline_options=PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session=save_main_session
    pipeline_options.view_as(StandardOptions).streaming=True

   # table_spec = bigquery.TableReference(known_args.outputTable)

    p=beam.Pipeline(runner=known_args.runner,options=pipeline_options)
    if known_args.inputSub:
       message=(
            p|beam.io.ReadFromPubSub(subscription=known_args.inputSub,with_attributes=True))
    else:
       message=(
           p|beam.io.ReadFromPubSub(topic=known_args.inputTopic,with_attributes=True))

    mainData,failure_extractElement=(
        message |'split'>>beam.ParDo(extractElement()).with_outputs(OUTPUT_TAG_FAILURE,main='outputs')
    )

    enrichData,failure_enrich=(
        mainData |'enrich by bigquery client' >> beam.ParDo(enrichByBQClient()).with_outputs(OUTPUT_TAG_FAILURE,main='outputs')
    )

    # The following code (from next line to ########### )leverage single output and retrieve different tag by processData[TAG] to get data.
    # processData=(
    #     enrichData | 'business logic' >> beam.ParDo(businessLogic()).with_outputs(
    #                             OUTPUT_TAG_FAILURE,
    #                             OUTPUT_TAG_COMPLETE,
    #                             OUTPUT_TAG_INCOMPLETE,
    #                             OUTPUT_TAG_NEW
    #     )
    # )
    #
    # completePipeline=(
    #     processData[OUTPUT_TAG_COMPLETE]
    #     |'format complete data output'>> beam.ParDo(format_result_for_bq())
    #     |'write complete data to bq' >> beam.io.WriteToBigQuery(
    #         table='agolis-allen-first:experiment.dataflow_duplicate',
    #         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    #     )
    # )
    #
    # insertPipeline=(
    #     processData[OUTPUT_TAG_NEW]
    #     |'format new data output'>> beam.ParDo(format_result_for_bq())
    #     |'write new data to bq' >> beam.io.WriteToBigQuery(
    #         table='agolis-allen-first:experiment.dataflow_enrich',
    #         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    #     )
    # )
    #
    # inCompletePipeline,failure_updateRow =(
    #     processData[OUTPUT_TAG_INCOMPLETE]
    #     |'update incomplete data in bq' >> beam.ParDo(updateRow()).with_outputs(OUTPUT_TAG_FAILURE,main='outputs')
    # )
    #
    # # flattern returns [(xxx)]
    # all_failure=(failure_extractElement,failure_enrich,processData[OUTPUT_TAG_FAILURE],failure_updateRow)\
    #             |"All Failure PCollection" >> beam.Flatten()\
    #             |"print all" >> beam.ParDo(format_data_for_pb())\
    #             |"send back to pubsub" >> beam.io.WriteToPubSub(
    #                 topic="projects/agolis-allen-first/topics/bigquery_demo",
    #                 with_attributes=True
    #             )
    ##################################

    #
    # The following code leverage tags for different output directly.
    newData,completeData,inCompleteData,failureData=(
        enrichData | 'business logic' >> beam.ParDo(businessLogic()).with_outputs(
                                OUTPUT_TAG_NEW,
                                OUTPUT_TAG_COMPLETE,
                                OUTPUT_TAG_INCOMPLETE,
                                OUTPUT_TAG_FAILURE
        )
    )

    newPipeline=(
        newData
        |'format new data output'>> beam.ParDo(format_result_for_bq())
        |'write new data to bq' >> beam.io.WriteToBigQuery(
            table='agolis-allen-first:experiment.dataflow_enrich',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR
        )
    )
    newPipeline_err=newPipeline[beam.io.gcp.bigquery.BigQueryWriteFn.FAILED_ROWS]

    completePipeline=(
        completeData
        |'format complete data output'>> beam.ParDo(format_result_for_bq())
        |'write complete data to bq' >> beam.io.WriteToBigQuery(
            table='agolis-allen-first:experiment.dataflow_duplicate',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR
        )
    )
    completePipeline_err=completePipeline[beam.io.gcp.bigquery.BigQueryWriteFn.FAILED_ROWS]

    inCompletePipeline,failure_updateRow =(
        inCompleteData
        |'update incomplete data in bq' >> beam.ParDo(updateRow()).with_outputs(OUTPUT_TAG_FAILURE,main='outputs')
    )

    all_failure=(failure_extractElement,failure_enrich,failureData,failure_updateRow)\
                |"All Failure PCollection" >> beam.Flatten()\
                |"parse all failure all" >> beam.ParDo(format_data_for_pb())\
                |"send all back to pubsub" >> beam.io.WriteToPubSub(
                    topic="projects/agolis-allen-first/topics/bigquery_demo",
                    with_attributes=True
                )

    bq_failure=(newPipeline_err,completePipeline_err) \
                |"BQ Failure PCollection" >> beam.Flatten()\
                |"parse bq failure" >> beam.ParDo(parseWriteResult())\
                |"send bq error back to pubsub" >> beam.io.WriteToPubSub(
                    topic="projects/agolis-allen-first/topics/bigquery_demo",
                    with_attributes=True
                )
    ##################################
    p.run().wait_until_finish()

if __name__ == '__main__':
    path_to_credential = '/Users/wangez/Downloads/GCP_Credentials/agolis-allen-first-13f3be86c3d1.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential
    logging.getLogger().setLevel(logging.INFO)

    OUTPUT_TAG_NEW = 'new'
    OUTPUT_TAG_INCOMPLETE = 'inComplete'
    OUTPUT_TAG_COMPLETE = 'complete'
    OUTPUT_TAG_FAILURE = 'failure'

    run()
