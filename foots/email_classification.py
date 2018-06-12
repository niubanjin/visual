from pymongo  import MongoClient
from datetime import time
from re import compile
from functools import reduce
import os
import re




def get_notice_email(myCollection_email,myCollection_inner):
    '''
    获取告警邮件
    '''
    i = 0
    print("正在查询告警邮件...")
    notice_data = myCollection_email.find({"From(address)":{"$regex":"@hack","$options":"$i"},
                                            "To(address)":{"$regex":"@hack","$options":"$i"},
                                            "$or":[{"Subject":{"$regex":"\\[warning","$options":"$i"}},
                                            {"Subject":{"$regex":"\\[success","$options":"$i"}},
                                            {"Subject":{"$regex":"\\[failed","$options":"$i"}}]
                                            },
                                            {"Subject":1,"Date Sent":1,"Creator Name":1})
    print("正在将告警邮件插入数据库中...")
    for row in notice_data:
        i+=1
        # myCollection_inner.insert({
        #     'Subject' : row['Subject'],
        #     'Sent' : row['Date Sent'],
        #     'Creator' : row['Creator Name'],
        #     'email_id' : row['_id'],
        #     'Type' : "告警邮件"
        # })
        yield row["_id"]
    print("告警邮件共有",i)

def get_confluence_email(myCollection_email,myCollection_inner):
    '''
    获取会议邮件
    '''
    i = 0
    print("正在查询会议邮件...")
    confluence_data = myCollection_email.find({"From(address)":{"$regex":"@hack","$options":"$i"},
                                                "To(address)":{"$regex":"@hack","$options":"$i"},
                                                "Subject":{"$regex":"\\[confluence","$options":"$i"}},
                                                {"Subject":1,"Date Sent":1,"Creator Name":1})
    print("正在将会议邮件插入数据库中...")
    for row in confluence_data:
        i+=1
        # myCollection_inner.insert({
        #     'Subject' : row['Subject'],
        #     'Sent' : row['Date Sent'],
        #     'Creator' : row['Creator Name'],
        #     'email_id' : row['_id'],
        #     'Type' : "会议邮件"
        # })
        yield row["_id"]
    print("会议邮件共有",i)
        
def get_support_email(myCollection_email,myCollection_inner):
    '''
    获取服务邮件
    '''
    i = 0
    print("正在查询服务邮件...")
    support_data = myCollection_email.find({"From(address)":{"$regex":"@hack","$options":"$i"},
                                                "To(address)":{"$regex":"@hack","$options":"$i"},
                                                "Subject":{"$regex":"\\[support","$options":"$i"}},
                                                {"Subject":1,"Date Sent":1,"Creator Name":1})
    print("正在将服务邮件插入数据库中...")
    for row in support_data:
        i+=1
        # myCollection_inner.insert({
        #     'Subject' : row['Subject'],
        #     'Sent' : row['Date Sent'],
        #     'Creator' : row['Creator Name'],
        #     'email_id' : row['_id'],
        #     'Type' : "服务邮件"
        # })
        yield row["_id"]
    print("服务邮件共有",i)

def get_other_email(myCollection_email,myCollection_inner,email_data):
    '''
    获取其他工作邮件
    '''
    i = 0
    print("正在查询其他工作邮件...")
    data_reader = myCollection_email.find({"From(address)":{"$regex":"@hack","$options":"$i"},
                                            "To(address)":{"$regex":"@hack","$options":"$i"}},
                                            {"Subject":1,"Date Sent":1,"Creator Name":1},no_cursor_timeout = True)
    print("正在将其他工作邮件插入数据库中...")
    for row in data_reader:
        if row['_id'] not in email_data:
            i+=1
            print(i)
            myCollection_inner.insert({
            'Subject' : row['Subject'],
            'Sent' : row['Date Sent'],
            'Creator' : row['Creator Name'],
            'email_id' : row['_id'],
            'Type' : "其他工作邮件"
        })
        else:
            pass
    print("其他邮件共有",i)



def get_outer_email(myCollection_email,myCollection_outer):
    '''
    获取外部邮件
    '''
    i = 0
    print("正在查询外部邮件...")
    outer_email = myCollection_email.find({
        "$or":[{
            "From(address)":
            {
                "$not":re.compile("@hack")}},
                {"From(address)":
                    {
                        "$regex":"@hack","$options":"$i"},
                        "To(address)":{
                            "$not":re.compile("@hack")
                            }
                    }
                    ]
                },no_cursor_timeout = True)
    print("正在将外部邮件插入数据库中")
    for row in outer_email:
        if row["To(address)"] != "" and row["From(address)"] != "":
            i+=1
            if "@hack" not in row["From(address)"]:
                suffix_type = row["From(address)"].split("@")[1]
            else:
                if ";" not in row["To(address)"]:
                    suffix_type = row["To(address)"].split("@")[1]
                else:
                    for item in row["To(address)"].split():
                        if "@hack" not in item:
                            suffix_type = item.split("@")[1]
        else:
            suffix_type = ""   
        try:
            myCollection_outer.insert({
                'Subject' : row['Subject'],
                'Sent' : row['Date Sent'],
                'Creator' : row['Creator Name'],
                'email_id' : row['_id'],
                'Type' : suffix_type
            })
        except TypeError as e:
            print(e)
            print(row)
            input()
    print("外部邮件共有",i)

def main():
    conn = MongoClient('127.0.0.1',27017)
    mydb = conn.test
    myCollection_email = mydb.email_data
    myCollection_inner = mydb.inner_email
    myCollection_outer = mydb.outer_email
    email_data = []

    # get_support_email(myCollection_email,myCollection_inner)
    # get_confluence_email(myCollection_email,myCollection_inner)
    # get_notice_email(myCollection_email,myCollection_inner)
    # [email_data.append(row) for row in get_support_email(myCollection_email,myCollection_inner)]
    # [email_data.append(row) for row in get_confluence_email(myCollection_email,myCollection_inner)]
    # [email_data.append(row) for row in  get_notice_email(myCollection_email,myCollection_inner)]
    # get_other_email(myCollection_email,myCollection_inner,email_data)
    get_outer_email(myCollection_email,myCollection_outer)

if __name__ == "__main__":
    main()



