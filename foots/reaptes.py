from pymongo  import MongoClient
import time as t
from collections import defaultdict

conn = MongoClient('127.0.0.1',27017)
mydb = conn.test
myCollect = mydb.email_data
str_list = {}
remove_list = []
email_dict = defaultdict(list)
i=0
j=0
data_reader = myCollect.find({},{'Date Sent':1,'From(address)':1,'To(address)':1,'Creator Name':1})
t.clock()
#算法一
print("开始处理数据")
for row in data_reader:
    str_address = "{0},{1},{2},{3}".format(row['From(address)'],row['To(address)'],row['Creator Name'],row['Date Sent'])
    email_dict[str_address].append(row['_id'])
    i+=1
print("去除重复数据中")
for keys in email_dict.keys():
    if email_dict[keys].__len__()>1:
        length = len(email_dict[keys])
        for k in range(1,length):
            j+=1
            myCollect.remove({"_id":email_dict[keys][k]})
       
print(t.clock())
print("已处理",i,"条数据")
print("已删除",j,"条数据")          

   


    




