from pymongo  import MongoClient
from time import strftime
import re
from django.http import JsonResponse

# conn = MongoClient("127.0.0.1",27017)
# mydb = conn.test
# topic_lists = []
# myCollection = mydb.topic_list
# myCountCollection = mydb.year_subject
# data_reader = myCollection.find({},{"subject":1}).sort([("count",-1)]).limit(20)
# [topic_lists.append(item["subject"])for item in data_reader if item["subject"] != ""]  
# for item in topic_lists:
#     suibject_reader = myCountCollection.find({"name":item})





