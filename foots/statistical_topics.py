from pymongo  import MongoClient
from functools import reduce
import re
import pandas as pd
from method import parse_subject,subject_time



def main(power):
    conn = MongoClient('127.0.0.1',27017)
    mydb = conn.test
    myOuterCollection = mydb.outer_email
    myInnerCollection = mydb.inner_email
    mySubjectCollection = mydb.topic_list
    if power == "year":
        myCollection = mydb.year_subject
    elif power == "month":
        myCollection = mydb.month_subject
    else:
        print("无效的参数power",power)
        input()
    subject_count= {}
    subject_data = mySubjectCollection.find({},{"subject":1},no_cursor_timeout = True)
    for row in subject_data:
        subject_count[row["subject"]] ={} 
    # 获取外部邮件
    outer_data_reader = myOuterCollection.find({},
            {
                "Sent":1,
                "Subject":1
                })
    # 处理外部邮件中的主题
    print("处理外部邮件中...")
    for row in outer_data_reader:
        if row["Subject"] != "":
            subject = parse_subject(row["Subject"])
            time = subject_time(row["Sent"],power)
            if subject != "":
                try:
                    subject_count[subject][time]["count"] += 1
                except KeyError as e:
                    subject_count[subject][time] = {
                        "count": 1,
                        "type" : "外部"
                    }
            else:
                pass
        else:
            pass
    #获取内部邮件
    inner_data_reader = myInnerCollection.find({},
            {
                "Sent":1,
                "Subject":1
                })
    #处理内部邮件中的主题
    print("处理内部邮件中...")
    for row in inner_data_reader:
        if row["Subject"] != "":
            subject = parse_subject(row["Subject"])
            time = subject_time(row["Sent"],power)
            if subject != "":
                try:
                    subject_count[subject][time]["count"] += 1
                except KeyError as e:
                    subject_count[subject][time] = {
                        "count": 1,
                        "type" : "内部"
                    }
            else: 
                pass
        else:
            pass
    count = 0
    print("插入数据库中...")
    for keys in subject_count.keys():
        if keys != "":
            for times_keys in subject_count[keys].keys():
                count += 1
                myCollection.insert({
                    "name" : keys,
                    "time" : times_keys,
                    "count" : subject_count[keys][times_keys]["count"],
                    "type" : subject_count[keys][times_keys]["type"]
                })
        else:
            pass
    print("处理完成，共有",count,"条数据")
if __name__ == "__main__":
    main("year")