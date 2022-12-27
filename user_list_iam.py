from google.oauth2 import service_account
import googleapiclient.discovery


if __name__ == '__main__':
    PROJECT_ID = 'agolis-allen-first'
    path_to_credential = '/Users/wangez/Downloads/agolis-allen-first-2a651eae4ca4.json'
    SCOPES = ['https://www.googleapis.com/auth/cloud-platform']

    credentials = service_account.Credentials.from_service_account_file(
        filename=path_to_credential,
        scopes=SCOPES)

    service = googleapiclient.discovery.build(
        'cloudresourcemanager',
        'v3',
        credentials=credentials)

    resource = 'projects/' + PROJECT_ID

    response = service.projects().getIamPolicy(resource=resource, body={}).execute()

    for binding in response['bindings']:
        print('Role:', binding['role'])

        for member in binding['members']:
            print(member)