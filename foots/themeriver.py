from pymongo import MongoClient
from time import strftime
import re
from django.http import JsonResponse
from pandas import Series

def main(mydb):
    '''
    获取各种类型下的邮件
    '''
    time_list = []
    myThemeRiver = mydb.theme_river
    myInnerCollection = mydb.inner_email
    myOuterCollection = mydb.outer_email
    # inner_reader = myInnerCollection.aggregate([{"$group":{"_id":"$Type","num":{"$sum":1},"list":{"$addToSet":"$Sent"}}}])
    inner_reader = myInnerCollection.find({})
    print("处理内部邮件类型中...")
    type_list = get_email(inner_reader)
    outer_reader = myOuterCollection.find({},no_cursor_timeout = True)
    print("处理外部邮件中...")
    for row in outer_reader:
        # time_list.append(strftime("%Y-%m-%d",tuple(row["Sent"])))
        time_list.append(strftime("%Y-%m",tuple(row["Sent"])))
    ds = Series(time_list)
    counts = ds.value_counts()
    for date,count in counts.iteritems():
        type_list.append([
            date,
            count,
            "外部邮件"
        ])
    print("正在存入数据库中...")
    for row in type_list:
        myThemeRiver.insert({
            "data":row
        })
    

def get_email(data_reader):
    '''
    处理邮件类型
    '''
    type_list = []
    email_list = []
    for row in data_reader:
        # for i in range(row["list"].__len__()):
        #     row["list"][i] = strftime("%Y-%m-%d",tuple(row["list"][i]))
        # ds = Series(row["list"])
        # counts = ds.value_counts()
        # for date,count in counts.iteritems():
        #     type_list.append([
        #         date,
        #         count,
        #         row["_id"]
        #     ])
        email_list.append(strftime("%Y-%m",tuple(row["Sent"])))
    ds = Series(email_list)
    counts = ds.value_counts()
    for date,count in counts.iteritems():
        type_list.append([
            date,
            count,
            "内部邮件"
        ])
    return type_list


if __name__ == "__main__":
    print("开始处理数据")
    conn = MongoClient("127.0.0.1",27017)
    mydb = conn.test
    main(mydb)