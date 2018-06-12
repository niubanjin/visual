from pymongo  import MongoClient
from functools import reduce
import re
from django.http import request
import pandas as pd
from method import parse_subject
# def Stacked(request):
#     conn = MongoClient("127.0.0.1",27017)
#     mydb = conn.test
#     myColletion = mydb.year_subject
#     myTopicCollection = mydb.topic_list
#     topic_data = myTopicCollection.find({}).sort([("count",-1)]).limit(10)
#     topic_list = []
#     time_list = ["2010","2011","2012","2013","2014","2015"]
#     temp_dict = {}
#     data_list = []
#     for row in time_list:
#         temp_dict[row] = 0
#     for row in topic_data:
#         topic_list.append(row["subject"])
#     for topics in topic_list:
#         temp_list = []
#         email_data = myColletion.find({"name":topics}).sort([("time",1)])
#         for row in email_data:
#             temp_dict[row["time"]] += row["count"]
#         [temp_list.append(temp_dict[item]) for item in time_list]
#         data_list.append({
#             "name":row["name"],
#             "data": temp_list
#         })
#     return JsonResponse({
#         "topic" : topic_list,
#         "time" : time_list,
#         "data" : data_list
#     })
conn = MongoClient("127.0.0.1",27017)
mydb = conn.test
myCollection = mydb.month_subject
type_list = []
inner_temp_dict = {}
outer_temp_dict = {}
time_list = []
inner_list = []
outer_list = []
# outer_data_reader = myCollection.find({"type":"外部"})
# inner_data_reader = myCollection.find({"type":"内部"})
data_reader = myCollection.find({},no_cursor_timeout = True)
for row in data_reader:
    # if row["data"][0]>"2011-01":
    #     type_list.append(row["data"])
    # else:
    #     pass
    time_list.append(row["time"])
time_list = list(set(time_list))
time_list.sort()

for keys in time_list:
    inner_temp_dict[keys] = 0
    outer_temp_dict[keys] = 0
for row in data_reader:
    if row["type"] == "内部":
        inner_temp_dict[row["time"]] += row["count"]
    else:
        outer_temp_dict[row["time"]] +=row["count"]
[inner_list.append(inner_temp_dict[keys]) for keys in time_list]
[outer_list.append(outer_temp_dict[keys]) for keys in time_list]
# return JsonResponse({
#     "time":time_list,
#     "inner_data" : inner_list,
#     "outer_data" : outer_list
# })