from google.cloud import bigquery
from google.iam.v1 import iam_policy_pb2,policy_pb2
from google.cloud import datacatalog_v1
from google.cloud.bigquery import datapolicies_v1
import os

if __name__ == '__main__':
    path_to_credential = '/Users/wangez/Downloads/agolis-allen-first-2a651eae4ca4.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential

    dc_client=datacatalog_v1.PolicyTagManagerClient()
    dp_client=datapolicies_v1.DataPolicyServiceClient()
    client=bigquery.Client()
    sql='SELECT ccore FROM `agolis-allen-first.ELM.cpu_model` LIMIT 1'
    query_job = client.query(sql)

    r'''
        query_job.referenced_tables contains all tables referred in a query.
        so we loop through all the columns in the reference table:
            if the field has policy attached:
                if current use does not have fine grained reader role on the policy
                    add the column to list col_with_policy 
        so the list will contains all field which has attached policy and current user does not have 
        finegraninedReader role on it. 
    '''

    col_with_policy=[]
    for table in query_job.referenced_tables:
        tb=client.get_table(table)
        for schemaField in tb.schema:
            if schemaField.policy_tags is not None:
                request=datacatalog_v1.GetPolicyTagRequest(
                   name=schemaField.policy_tags.names[0]
                )
                policy=dc_client.get_policy_tag(request)

                request=iam_policy_pb2.TestIamPermissionsRequest(
                    resource=policy.name,
                    permissions=['datacatalog.categories.fineGrainedGet']
                )
                policy_permission_result=dc_client.test_iam_permissions(request=request).permissions
                if len(policy_permission_result) == 0:
                    col_with_policy.append(schemaField)

    result =query_job.result()

    r'''
        If we notice the message poped up in Bigquery UI - 
        'Your query might contain masked data due to the data policies applied to some of the columns.' 
        in which there is word 'might'. 
        
        The query result object contains a "schema" attribute as well, but we can not tell which table 
        does certain field belong to. I am afraid this might be reason that word 'might' happened in that message. 
        
        The logic below is to 
        loop through result schema
            if certain result schemaField has same name and type as certain field in the col_with_policy
                get the policyTag
                find the data policy with the policy
                check current use has masked reader role to the data policy or not
                if true:
                    print message "there might be masked data in the result"
                    break the loop             
    '''
    has_masked_data=False
    for result_schemaField in result.schema:
        for schemaField in col_with_policy:
            if result_schemaField.name==schemaField.name and result_schemaField.field_type == schemaField.field_type:
                request=datacatalog_v1.GetPolicyTagRequest(
                   name=schemaField.policy_tags.names[0]
                )
                policy=dc_client.get_policy_tag(request)

                request=datapolicies_v1.ListDataPoliciesRequest(
                    parent='projects/agolis-allen-first/locations/us',
                    filter='policy_tag:{}'.format(policy.name)
                )
                data_policy_list=dp_client.list_data_policies(request=request)
                data_policy=data_policy_list._response.data_policies.pop().name

                request = iam_policy_pb2.TestIamPermissionsRequest(
                    resource=data_policy,
                    permissions=["bigquery.dataPolicies.maskedGet"]
                )

                permission_result=dp_client.test_iam_permissions(request).permissions
                if len(permission_result) > 0:
                    has_masked_data=True
                    print('There might be masked data in the result')
                    break

    print('x')

