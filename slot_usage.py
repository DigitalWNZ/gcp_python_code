from google.cloud.bigquery_reservation_v1 import *
from google.api_core import retry
from google.cloud import bigquery
from google.cloud.bigquery.job import QueryJobConfig
import time,os,json


def run_query(project_id):
    client = bigquery.Client(project=project_id)
    config = QueryJobConfig(use_query_cache=False,
                            use_legacy_sql=False)

    query = '''
    create or replace table aftership.temp_t as select * from `agolis-allen-first.aftership.ping` limit 2;
    '''
    job = client.query(query, job_config=config)
    jobid = job.job_id
    print(jobid)
    job = client.get_job(jobid)  # API request
    print(job.state)
    # Print selected job properties
    index = 0
    while job.state == 'RUNNING':
        print('check for {} time'.format(str(index)))
        index += 1
        job = client.get_job(jobid)
        print("Details for job {} running in {}:".format(jobid, "US"))
        print(
            "\tType: {}\n\tState: {}\n\tCreated: {}\n\tUser: {}".format(
                job.job_type, job.state, job.created, job.user_email
            )
        )
        time.sleep(20)
    return True;

def purchase_commitment(res_api,parent_arg,commitment_name=None,slots=100):
  commit_config = CapacityCommitment(plan='FLEX', slot_count=slots)
  commit = res_api.create_capacity_commitment(parent=parent_arg,
                                              # capacity_commitment_id=commitment_name,
                                              capacity_commitment=commit_config)

  print(commit)
  commit_status=res_api.get_capacity_commitment(name=commit.name).state.name
  if commit_status != "ACTIVE":
      time.sleep(5);
      commit_status=res_api.get_capacity_commitment(name=commit.name).state.name

  return commit.name


def create_reservation(res_api,reservation_name, parent_arg,slots=100):
  res_config = Reservation(slot_capacity=slots, ignore_idle_slots=False)
  res = res_api.create_reservation(parent=parent_arg,
                                   reservation_id=reservation_name,
                                   reservation=res_config)
  print(res)
  return res.name


def create_assignment(res_api,reservation_id, user_project,assignment_name=None):
  assign_config = Assignment(job_type='QUERY',
                             assignee='projects/{}'.format(user_project))
  assign = res_api.create_assignment(parent=reservation_id,
                                     # assignment_id=assignment_name,
                                     assignment=assign_config)
  print(assign)
  return assign.name

def cleanup(res_api,assignment_id, reservation_id, commit_id):
  res_api.delete_assignment(name=assignment_id)
  res_api.delete_reservation(name=reservation_id)
  res_api.delete_capacity_commitment(name=commit_id,
                                     retry=retry.Retry(deadline=90,
                                                       predicate=Exception,
                                                       maximum=2))


def main_process(request):
    # path_to_credential = '/Users/wangez/Downloads/agolis-allen-first-2a651eae4ca4.json'
    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential
    replies = []
    request_json = request.get_json(silent=True)
    calls = request_json['calls']
    for call in calls:
        admin_project_id=call[0].split(',')[0]
        region = call[0].split(',')[1]

    res_api = ReservationServiceClient()
    parent_arg = "projects/{}/locations/{}".format(admin_project_id,region)
    # commitment_name="commitment-test"
    reservation_name="resevation-test"
    # assignment_name="assignment-test"

    start = time.time()
    slots = 100  # increase in increments of 500
    commit_id = purchase_commitment(res_api=res_api,parent_arg=parent_arg)
    res_id = create_reservation(res_api=res_api,reservation_name=reservation_name,parent_arg=parent_arg, slots=slots)
    assign_id = create_assignment(res_api=res_api,reservation_id=res_id,user_project=admin_project_id)
    time.sleep(60)  # assignment warmup
    run_query(admin_project_id)
    cleanup(res_api,assign_id,res_id,commit_id)

    end = time.time()
    print("reservation ran for ~{} seconds".format((end - start)))
    replies.append({"result":"Done"})
    return json.dumps({
        "replies":[json.dumps(reply) for reply in replies]
    })

if __name__ == '__main__':
    # admin_project_id="agolis-allen-first"
    # region="US"
    # main_process(admin_project_id,region)
    replies = []
    replies.append({'result': 'Done'})
    print(json.dumps({'replies':[json.dumps(reply) for reply in replies]}))

    # {"admin_project_id": "agolis-allen-first", "region": "US"}


