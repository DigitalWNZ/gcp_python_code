from google.cloud import datacatalog_v1
from google.iam.v1 import iam_policy_pb2,policy_pb2
from google.cloud import bigquery
from google.cloud.bigquery import datapolicies_v1
import google.cloud.bigquery.datapolicies_v1.types
import os

if __name__ == '__main__':
    path_to_credential = '/Users/wangez/Downloads/agolis-allen-first-2a651eae4ca4.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential


    project_id: str = "agolis-allen-first"
    location_id: str = "us"
    display_name: str = "example-taxonomy"

    client = datacatalog_v1.PolicyTagManagerClient()

    # Create taxonomy
    parent = datacatalog_v1.PolicyTagManagerClient.common_location_path(
        project_id, location_id
    )
    taxonomy = datacatalog_v1.Taxonomy()
    taxonomy.display_name = display_name
    taxonomy.description = "This Taxonomy represents ..."
    taxonomy = client.create_taxonomy(parent=parent, taxonomy=taxonomy,)
    print(f"Created taxonomy {taxonomy.name}")

    # Create policy tag
    parent_value=taxonomy.name
    policy_tag=datacatalog_v1.PolicyTag()
    policy_tag.display_name="example policy tag"
    policy_tag = client.create_policy_tag(parent=parent_value,policy_tag=policy_tag)
    print(f"Created policy_tag {policy_tag.name}")

    # add findgrained reader role to policy tag
    policyTag = policy_tag.name
    # The policy tag below are for test purpose
    # policyTag="projects/agolis-allen-first/locations/us/taxonomies/5795552885058236759/policyTags/5837399662345023402"

    request=iam_policy_pb2.GetIamPolicyRequest(
        resource=policyTag
    )
    iam_policy=client.get_iam_policy(request=request)

    user="user:wangez@google.com"
    role="roles/datacatalog.categoryFineGrainedReader"
    nb=policy_pb2.Binding(role=role,members={user})
    iam_policy.bindings.append(nb)
    set_request=iam_policy_pb2.SetIamPolicyRequest(
        resource=policyTag,
        policy=iam_policy
    )
    client.set_iam_policy(request=set_request)

    dp_client= datapolicies_v1.DataPolicyServiceClient()
    # add data policy to policy tag
    # The policy tag are for test purpose
    # policyTag = "projects/agolis-allen-first/locations/us/taxonomies/5795552885058236759/policyTags/5837399662345023402"
    dp=datapolicies_v1.DataPolicy()
    dp.data_policy_type=datapolicies_v1.types.DataPolicy.DataPolicyType.DATA_MASKING_POLICY
    dp.data_policy_id='test_dp3'
    dp.policy_tag=policyTag
    dmp=datapolicies_v1.DataMaskingPolicy()
    dmp.predefined_expression=datapolicies_v1.types.DataMaskingPolicy.PredefinedExpression.DEFAULT_MASKING_VALUE
    dp.data_masking_policy=dmp

    req=datapolicies_v1.CreateDataPolicyRequest(
        parent='projects/agolis-allen-first/locations/us',
        data_policy = dp
    )
    dp=dp_client.create_data_policy(request=req)
    print(f"Created data policy {dp.name}")

    # set IAM for the data policy
    dp_name=dp.name
    # the dp_name below is for test purpose
    # dp_name='projects/agolis-allen-first/locations/us/dataPolicies/test_dp3'
    request=iam_policy_pb2.GetIamPolicyRequest(
        resource=dp_name
    )
    iam_policy=dp_client.get_iam_policy(request=request)

    user="user:wangez@google.com"
    # gcp role reference--https://cloud.google.com/iam/docs/understanding-roles
    role="roles/bigquerydatapolicy.maskedReader"
    nb=policy_pb2.Binding(role=role,members={user})
    iam_policy.bindings.append(nb)
    set_request=iam_policy_pb2.SetIamPolicyRequest(
        resource=dp_name,
        policy=iam_policy
    )
    iam_policy=dp_client.set_iam_policy(request=set_request)

    #update table schema with policy tag
    # The policyTag are for test purpose
    # policyTag = "projects/agolis-allen-first/locations/us/taxonomies/5795552885058236759/policyTags/5837399662345023402"
    bq_client=bigquery.Client()
    tb=bq_client.get_table('agolis-allen-first.ELM.cpu_model')

    old_schema=tb.schema
    new_schema = []
    for schemaField in old_schema:
        if schemaField.name=='acore':
            new_schemaField=bigquery.SchemaField(name=schemaField.name,field_type=schemaField.field_type,policy_tags=bigquery.PolicyTagList([policyTag]))
            new_schema.append(new_schemaField)
        else:
            new_schema.append(schemaField)

    tb.schema=new_schema
    bq_client.update_table(tb,['schema'])


    print('x')


