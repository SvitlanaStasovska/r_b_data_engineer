from datetime import datetime
import math


# dd = '2023-11-13T12:02:50'
# dd_ = dd.replace('T',' ')

# print("date_string =", dd)
# print("type of date_string =", type(dd))

# operation_date = datetime.strptime(dd.replace('T',' '), '%Y-%m-%d %H:%M:%S')
# print(operation_date)
#

# names = {
# 'head_of_file' : '[column_1,column_2,column_n]'
# }
# str_columns = names['head_of_file']
# len_ = len(str_columns)
# i = 0
# for i in range(len_):
#     print(str_columns[i])
#
########
#
# arr = [('000000000000000000000000',)]
# print(arr[0][0])

#print(math.floor(0.3), round(1.4))

cnt_loads = round(2.9)
a1 = math.ceil(2.2)
a2 = math.ceil(0.6)
if cnt_loads == 0: cnt_loads = 1

print(cnt_loads, a1, a2)
