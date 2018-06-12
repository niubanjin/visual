from pymongo  import MongoClient
from time import strftime 
from time import strptime 
from pandas import Series
import time
import pandas as  pd
from re import compile

conn = MongoClient('127.0.0.1',27017)
mydb = conn.test
myCollection = mydb.email_data
myCollectionconn = mydb.list_connection
i = 0
sent_data = []
sent_list = []
data_reader = myCollection.find({},{'_id':0,'Date Sent':1},no_cursor_timeout=True).limit(1000)
for row in data_reader:
    sent_list.append(tuple(row['Date Sent']))
for row in sent_list:
    try:
        i = i+1
        times = strftime("%Y-%m-%d",row)
        sent_data.append(times)
    except TypeError:
        print(type(row))
        input()
print("已处理",i,"条数据")
ds = Series(sent_data)
counts = ds.value_counts()
# grouped = ds['time'].groupby(ds['time'])
# counts = grouped.Series.value_counts(normalize=True)
for row in counts.iteritems():
    myCollectionconn.insert({
        'Date':row[0],
        'Count':row[1]
    })
    # if row[0] =="2010-01-12":
    #     print(row)