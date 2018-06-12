from pymongo  import MongoClient
from functools import reduce
import re
import pandas as pd
from method import parse_subject

def main():
    conn = MongoClient('127.0.0.1',27017)
    mydb = conn.test
    subject_list = []
    i = 0
    myCollection = mydb.email_data
    myTopicCollection = mydb.topic_list
    data_reader = myCollection.find({},{"Subject":1},no_cursor_timeout = True)
    print("正在处理数据...")
    for row in data_reader:
        if row["Subject"] != "":
            new_subject = parse_subject(row["Subject"])
            subject_list.append(new_subject)
        else:
            pass
    ds = pd.Series(subject_list)
    result = ds.value_counts()
    print("正在导入数据库中...")
    for topics,count in result.iteritems():
        i+=1
        if topics != "":
            myTopicCollection.insert({
                "subject":topics,
                "count":count 
            })  
        else:
            pass
    print("已处理数据",i,"条")
if __name__ == "__main__":
    main()