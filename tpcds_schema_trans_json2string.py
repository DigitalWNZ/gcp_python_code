from google.cloud import bigquery
import time
import os
import json

if __name__ == '__main__':
    dir_name='Bigquery_schema_json'
    file_list=os.listdir(dir_name)
    output_file=open('./Bigquery_schema_str/bigquery_schema_str.txt','a')
    for file in file_list:
        output_file.write('{}:\n'.format(file))
        schema_json=json.load(open('./{}/{}'.format(dir_name,file)))
        print(schema_json)
        schema_str='('
        for idx in range(0,len(schema_json)):
            element=schema_json[idx]
            schema_str=schema_str + element['name'] + ' ' + element['type'] + ','
        schema_str=schema_str[:len(schema_str)-1] + ') \n'
        output_file.write(schema_str)





