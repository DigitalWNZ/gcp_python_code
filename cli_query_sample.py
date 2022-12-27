import airflow
from airflow.operators import bash_operator, python_operator
from datetime import date, timedelta, datetime
from google.cloud import bigquery
import time

YESTERDAY = datetime.now() - timedelta(days=1)

HQL = """
create temporary table post_show_user as
   select  s1.uid
           ,count(s1.post_id) as show_post_num 
           ,sum(s1.show_cnt)  as show_cnt      
   from
       (
           select  uid
                   ,JSON_EXTRACT(moreinfo, '$.subject_id') as  subject_id
                   ,JSON_EXTRACT(moreinfo, '$.tag_id')     as label_id
                   ,JSON_EXTRACT(moreinfo, '$.post_id')     as post_id
                   ,count(1) as show_cnt
           from    hagotestbq.kaixindou.hago_mbsdkprotocol_original
           where   eventid = '20036879'
           and     dt = '20201205'
           and     JSON_EXTRACT(moreinfo, '$.function_id') = '"post_content_show"'
           and     uid > 0
           group by post_id,label_id,subject_id,uid
       )           s1
   group by        s1.uid
   ;

create temporary table post_user_click as
select          s1.uid
               ,count(if(function_id = '"post_more_click"',s1.post_id,null))   as click_cost_pv
               ,count(s1.post_id)                                            as click_pv     
from
   (
       select  dt
               ,uid
               ,JSON_EXTRACT(moreinfo, '$.function_id') as function_id
               ,JSON_EXTRACT(moreinfo, '$.post_id') as post_id
       from    hagotestbq.kaixindou.hago_mbsdkprotocol_original
       where   eventid = '20036879'
       and     dt = '20201205'
       and     JSON_EXTRACT(moreinfo, '$.function_id') in ('"post_head_click"','"post_content_click"','"post_more_click"','"post_share_click"','"post_comment_click"','"post_good_click"','"post_tag_click"','"post_hot_comment_click"')
       and     uid > 0
   )           s1
group by        s1.uid;


create temporary table tb_summary as
select          s1.uid
               ,abtest_report_type
               ,abtest
               ,abgroup
               ,test_type
               ,control_group
               ,nation
               ,ver
               ,cast(is_new_user as STRING)    as  is_new_user
               ,appkey
               ,if(s2.uid is not null,1,0)  as is_post_show    
               ,coalesce(show_post_num,0)        as show_post_num
               ,coalesce(show_cnt,0)             as show_cnt
               ,coalesce(click_cost_pv,0)        as click_cost_pv
               ,coalesce(click_pv,0)             as click_pv
from
   (
       select  uid
       from    post_show_user
   )           s1

   left join
   (
       select  uid
               ,show_post_num
               ,show_cnt
       from    post_show_user
   )           s2
   on          s1.uid = s2.uid

   left join
   (
       select  uid
               ,click_cost_pv
               ,click_pv
       from    post_user_click
   )           s4
   on          s1.uid = s4.uid
   inner join
   (
       select 
           cast(uid as int64)  as  uid
           ,abtest
           ,abgroup
           ,abtest_report_type
           ,test_type
           ,control_group
     from    hagotestbq.kaixindou.hago_dwd_user_abtest_uid_day_detail
     where   dt = '20201205'
   )           s5
   on          s1.uid = s5.uid
   inner join
   (
       select  uid
               ,nation
               ,ver
               ,is_new_user
               ,appkey
       from    hagotestbq.kaixindou.hago_dwd_user_uid_day_detail
       where   dt = '20201205'
   )           s6
   on          s1.uid = s6.uid
;

create temporary table tmp_post_interact as
select      abtest_report_type
           ,abtest
           ,abgroup
           ,test_type
           ,control_group
           ,coalesce(nation, 'all')         as nation
           ,coalesce(ver, 'all')            as  ver
           ,coalesce(is_new_user, 'all')    as  is_new_user
           ,coalesce(appkey, 'all')         as  appkey
           ,count(uid)                 as  users
           ,sum(is_post_show)          as  is_post_show
           ,sum(show_post_num)         as  show_post_num
           ,sum(show_cnt)              as  show_cnt
           ,sum(click_cost_pv)         as  click_cost_pv
           ,sum(click_pv)              as  click_pv
from        tb_summary
,UNNEST([nation, 'all']) as nation
,UNNEST([ver, 'all']) as ver
,UNNEST([is_new_user, 'all']) as is_new_user
,UNNEST([appkey, 'all']) as appkey
group by  abtest_report_type,abtest,abgroup,test_type,control_group,nation,ver,is_new_user,appkey
;
delete from hagotestbq.kaixindou.hago_dm_test_detail where dt = '{}';
insert into hagotestbq.kaixindou.hago_dm_test_detail(dt,nation,ver,is_new_user,appkey,abtest,abgroup,abtest_report_type,test_type,control_group,users,is_post_show,show_post_num,show_cnt,click_cost_pv,click_pv,par)
select '{}'                   as  dt
       ,nation
       ,ver
       ,is_new_user
       ,appkey
       ,abtest
       ,abgroup
       ,abtest_report_type
       ,test_type
       ,control_group
       ,users
       ,is_post_show
       ,show_post_num
       ,show_cnt
       ,click_cost_pv
       ,click_pv
       ,TIMESTAMP("{} 00:00:01+00") as par
from tmp_post_interact

""".format(datetime.strftime(datetime.now() - timedelta(days=1), "%Y%m%d"), datetime.strftime(datetime.now() - timedelta(days=1), "%Y%m%d"), datetime.strftime(datetime.now() - timedelta(days=1), "%Y-%m-%d"))


def task3():
   client = bigquery.Client()
   query_job=client.query(HQL,location="asia-east2")
   print('query submitted')
   jobid=query_job.job_id
   print(jobid)
   job = client.get_job(jobid, location="asia-east2")  # API request
   print(job.state)
   # Print selected job properties
   index = 0
   while job.state=='RUNNING':
       print('check for {} time'.format(str(index)))
       index +=1
       job = client.get_job(jobid, location="asia-east2")
       print("Details for job {} running in {}:".format(jobid, "US"))
       print(
           "\tType: {}\n\tState: {}\n\tCreated: {}\n\tUser: {}".format(
               job.job_type, job.state, job.created, job.user_email
           )
       )
       time.sleep(20)

default_args = {
   'owner': 'Composer Example',
   'depends_on_past': True,
   'email': [''],
   'email_on_failure': False,
   'email_on_retry': False,
   'retries': 1,
   'retry_delay': timedelta(minutes=5),
   'start_date': YESTERDAY,
}

dag = airflow.DAG(
       'task_scheduler1',
       'catchup=False',
       default_args=default_args,
       schedule_interval=timedelta(days=1))


t1 = bash_operator.BashOperator(
   task_id = 't1',
   bash_command = 'echo "t1 task"',
   dag = dag,
   )

t2 = bash_operator.BashOperator(
   task_id = 't2',
   bash_command = 'echo "t2 task"',
   depends_on_past = True,
   dag = dag,
   )

t3 = bash_operator.BashOperator(
   task_id = 't3',
   bash_command = 'echo "t3 task"',
   depends_on_past = True,
   dag = dag,
   )

t4 = python_operator.PythonOperator(
   task_id = 'hago_dm_test',
   python_callable = task3,
   depends_on_past = True,
   dag = dag,
   )

end = bash_operator.BashOperator(
   task_id = "end",
   bash_command = "echo task success...",
   depends_on_past = True,
   dag = dag,
   )

[t1, t2, t3] >> t4 >> end