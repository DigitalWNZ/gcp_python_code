# projected columns
#     projected columns - 放在select中的column
#     columns - 表中所有的columns
#     table - 表
#
# predicted
#     tables = where条件中所涉及表的描述
#     column = 真正使用的字段，但是CAST(m.jobStatistics.totalSlotMs AS INTEGER) > 100 是不能识别的。
import json

if __name__ == '__main__':
    f = open('sql_parse_result.json')
    res=json.load(f)

    partition_fields={}

    for table in res['predicates']['tables']:
        table_meta=table['table']
        table_id='.'.join([table_meta['projectId'],table_meta['datasetId'],table_meta['tableId']])
        time_Partition_Field=table['timePartitionField']
        range_Partition_Field = table['rangePartitionField']
        if time_Partition_Field is not None:
            partition_field=time_Partition_Field['field']
        elif range_Partition_Field is not None:
            partition_field=range_Partition_Field['field']
        else:
            partition_field=None
        partition_fields[table_id]=partition_field

    print(partition_fields)

    predicate_fields={}
    for column in res['predicates']['columns']:
        table_meta=column['table']
        table_id = '.'.join([table_meta['projectId'], table_meta['datasetId'], table_meta['tableId']])
        predicate_field = column['columnName']
        if table_id in predicate_fields:
            predicate_fields[table_id].append(predicate_field)
        else:
            predicate_fields[table_id]=[predicate_field]

    print(predicate_fields)

    for k,v in partition_fields.items():
        if v in predicate_fields[k]:
            print('partition column {} in table {} is used in predicate'.format(v,k))

