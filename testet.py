# from google.cloud import bigquery_datatransfer
# from google.protobuf import field_mask_pb2
#
# transfer_client = bigquery_datatransfer.DataTransferServiceClient()
#
# service_account_name = "abcdef-test-sa@abcdef-test.iam.gserviceaccount.com"
# transfer_config_name = "projects/1234/locations/us/transferConfigs/abcd"
#
# transfer_config = bigquery_datatransfer.TransferConfig(name=transfer_config_name)
#
# transfer_config = transfer_client.update_transfer_config(
#     {
#         "transfer_config": transfer_config,
#         "update_mask": field_mask_pb2.FieldMask(paths=["service_account_name"]),
#         "service_account_name": service_account_name,
#     }
# )
#
# print("Updated config: '{}'".format(transfer_config.name))

#
# def find_61st_number_optimized():
#     numbers = [1, 3, 9, 27, 81, 243]
#     sums = []
#
#     # 生成所有可能的二进制组合 (排除全0)
#     for i in range(1, 2**len(numbers)):
#         binary_str = format(i, '06b')  # 格式化为6位二进制字符串
#         s = 0
#         for j in range(len(numbers)):
#             if binary_str[j] == '1':
#                 s += numbers[j]
#         sums.append(s)
#
#     # 对数字和排序
#     sums.sort()
#
#     # 找到第61个数字
#     return sums[60]
#
# result = find_61st_number_optimized()
# print("第61个数是:", result)


import datetime
x=datetime.datetime.fromtimestamp(1721828354.459033).strftime('%Y-%m-%d %H:%M:%S')
print(x)

x=datetime.datetime.fromtimestamp(1721973399165).strftime('%Y-%m-%d %H:%M:%S')
print(x)