# -*- coding: utf-8 -*-

from pymongo import MongoClient
from django.http import JsonResponse
from foots.method import parsetext,getnum

# Create your views here.



def test(request):
    '''
    line
    '''
    conn = MongoClient('127.0.0.1',27017)
    mydb = conn.test
    myCollection = mydb.list_connection
    data_reader = myCollection.find().sort([("Date",1)])
    date_list = []
    count_list = []
    for row in data_reader:
        date_list.append(row['Date'])
        count_list.append(row['Count'])
    # count_list.append(1000) 
    return JsonResponse({
        'date': date_list,
        'data': count_list
        })
def pin(request):
    '''
    pin
    '''
    conn = MongoClient('127.0.0.1',27017)
    mydb = conn.test
    outer_count = 0
    inner_count = 0
    first_list = []
    outer_list = []
    second_list = []
    myCollection_inner_email = mydb.inner_email
    myCollection_outer_email = mydb.outer_email
    subject = {"内部邮件","外部邮件","告警邮件","会议邮件","支持邮件","其他工作邮件"}
    inner_count = myCollection_inner_email.find().count()
    outer_count = myCollection_outer_email.find().count()
    outer_email_reader = myCollection_outer_email.aggregate([
        {
            "$group":{
                "_id":"$Type",
                "num":{
                    "$sum" :1
                }
            }
        }
    ])
    for row in outer_email_reader:
        outer_email = {
            "name" : row["_id"],
            "value" : row["num"]
        }
        outer_list.append(outer_email)
    outer_list.sort(key = getnum,reverse = True)
    [second_list.append(outer_list[i]) for i in range(5)]
    [subject.add(outer_list[i]["name"]) for i in range(5)]
    inner_email_reader = myCollection_inner_email.aggregate([
        {
            "$group":{
                "_id":"$Type",
                "num":{"$sum":1}
                }
            }
    ])
    for row in inner_email_reader:
        inner_email = {
            "name" : row["_id"],
            "value" : row["num"]
        }
        second_list.append(inner_email)
    first_list.append({
        "name" : "内部邮件",
        "value" : inner_count
    })
    first_list.append({
        "name" : "外部邮件",
        "value" : outer_count
    })
    return JsonResponse({
        "first_data" : first_list,
        "second_data" : second_list,  
        "subject" : list(subject)
    })
def radar(request):
    '''
    radar
    '''
    conn = MongoClient('127.0.0.1',27017)
    mydb = conn.test
    myCollection = mydb.inner_staff
    myCollection_max = mydb.figure_cache
    staff_list = []
    i = 0
    standard = []
    data = []
    data_reader = myCollection.find({})
    max_data = myCollection_max.find_one({"name":"inner_staff"})
    max_keys = max_data.keys()
    for row in data_reader:
        staff_information = []
        i +=1
        for keys in max_keys:
            if keys != "_id" and keys != "name":
                if i == 1:
                    standard.append({
                        "name" : parsetext(keys),
                        "max" : max_data[keys]
                    })
                else:
                    pass
                if keys == "releation_people":
                    staff_information.append(len(row[keys]))
                else:
                    staff_information.append(row[keys])
            else:
                pass
        data.append({
            "name" : row["name"],
            "value" : staff_information
        })
        staff_list.append(row["name"])
    return JsonResponse({
        "staff_list" : staff_list,
        "standard" : standard,
        "data" : data
    })
def line(request):
    '''
    line
    '''
    conn = MongoClient("127.0.0.1",27017)
    mydb = conn.test
    myCollection = mydb.month_subject
    type_list = []
    inner_temp_dict = {}
    outer_temp_dict = {}
    temp_list = []
    time_list = []
    inner_list = []
    outer_list = []
    data_reader = myCollection.find({})
    for row in data_reader:
        time_list.append(row["time"])
        temp_list.append({
            "type":row["type"],
            "count":row["count"],
            "time":row["time"]
        })
    time_list = list(set(time_list))
    time_list.sort()

    for keys in time_list:
        inner_temp_dict[keys] = 0
        outer_temp_dict[keys] = 0
    for row in temp_list:
        if row["type"] == "内部":
            inner_temp_dict[row["time"]] += row["count"]
        else:
            outer_temp_dict[row["time"]] += row["count"]
        
    [inner_list.append(inner_temp_dict[keys]) for keys in time_list]
    [outer_list.append(outer_temp_dict[keys]) for keys in time_list]
    # return JsonResponse({
    #     "time" : time_list,
    #     "inner_data" : inner_list,
    #     "outer_data" : outer_list
    # })
    # github的line
    return JsonResponse({
        "code":0,
        "message":"ok",
        "data":{
            "time": time_list,
            "inner": inner_list,
            "outer":outer_list
        }
    })
        
def heatmap(request):
    '''
    heatmap
    '''
    conn = MongoClient("127.0.0.1",27017)
    mydb = conn.test
    time_list = []
    email_data = []
    data_list = []
    topics_list = []
    myMonthCollection = mydb.month_subject
    myCollection = mydb.topic_list
    data_reader = myCollection.find({},no_cursor_timeout = True).sort([("count",-1)]).limit(10)
    for row in data_reader:
        topics_list.append(row["subject"])
        month_reader = myMonthCollection.find({"name":row["subject"]},{"_id":0})
        for item in month_reader:
            time_list.append(item["time"])
            email_data.append({
                "name" : item["name"],
                "count" : item["count"],
                "time" : item["time"]
            })
    time_list = list(set(time_list))
    time_list.sort()
    for row in email_data:
        xLocation = time_list.index(row["time"])
        yLocation = topics_list.index(row["name"])
        count = row["count"]
        data_list.append([
            xLocation,
            yLocation,
            count
        ])
    # return JsonResponse({
    #     "topic" : topics_list,
    #     "time" : time_list,
    #     "data" : data_list
    # })
    return JsonResponse({
        'code': 0,
        'message': 'ok',
        'data':{
            "xLocation" : time_list,
            "yLocation" : topics_list,
            "data" : data_list
        }
    })
# def graph(request):
#     conn = MongoClient("127.0.0.1",27017)
#     mydb = conn.test
#     myconnCollection = mydb.
#     return JsonResponse()



def api(request):
    conn = MongoClient("127.0.0.1",27017)
    mydb = conn.test
    myCollection = mydb.theme_river
    type_list = []
    # data_reader = myCollection.find({"data":{"$not":{"$in":["外部邮件"]}}})
    data_reader = myCollection.find({})
    for row in data_reader:
        if row["data"][0]>"2011-01":
            type_list.append(row["data"])
        else:
            pass
    return JsonResponse({
        "data":type_list,
        "legend" : ["外部邮件","内部邮件"] 
    })
        
