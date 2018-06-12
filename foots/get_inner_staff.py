from pymongo  import MongoClient
from time import strftime 
from time import strptime 
from pandas import Series
import time
import pandas as  pd
from re import compile
from functools import reduce

def integration(address1,address2):
    '''
    将同一员工的不同邮箱的信息进行整合
    '''
    [address1['address'].append(item) for item in address2['address']]
    
    address1_importance = (address1['in_count']+address1['out_count'])*address1['importance']
    
    address2_importance = (address2['in_count']+address2['out_count'])*address2['importance']
    
    address1['in_count'] += address2['in_count']
    
    address1['out_count'] += address2['out_count']
    
    address1['importance'] = (address1_importance+address2_importance)/(address1['in_count']+address1['out_count'])
    
    address1['in_time'] = min(address1['in_time'],address2['in_time'])

    [address1['releation_people'].append(item) for item in address2['releation_people'] if item not in address1['releation_people']]
    return address1

def get_releation(staffs,myCollection_connection):
    '''
    获取联系人信息和收发邮件信息
    '''
    connection = myCollection_connection.find({'address':staffs['address']},{'connection':1})
    temp = []
    temp_in = 0
    temp_out = 0 
    for cs in connection:
        for item in cs['connection']['out']['list']:
            [temp.append(items) for items in item['to_address']]
        [temp.append(item['from_address']) for item in cs['connection']['in']['list']]
        temp_in += cs['connection']['in']['count']
        temp_out += cs['connection']['out']['count']
    return {
        'in' : temp_in,
        'out' : temp_out,
        'list' : temp
    }

def get_start(staffs,myCollection):
    '''
    获取员工的邮件首发量
    '''
    start_count = 0
    data_reader = myCollection.find({'Creator Name':staffs},{'From(display)':1})
    for row in data_reader:
        if row['From(display)'] != '':
            if staffs == row['From(display)']:
                start_count += 1
            else:
                pass
    return start_count

def inner_staff(data_reader,myCollection_connection):
    '''
    获得内部员工邮箱并去除边缘任务的邮箱，判定标准为收件数小于500且发送邮件数量为0
    '''
    inner_list = []
    for address in data_reader:
        list_in_count = 0
        list_out_count = 0
        connection_reader = myCollection_connection.find({'address':address['address']},{'connection':1})
        for row in connection_reader:
            list_in_count += row['connection']['in']['count']
            list_out_count += row['connection']['out']['count']
        list_count  = list_in_count + list_out_count
        #需要增加判断条件发件数量大于1，和收件数量大于500才是有效员工
        if list_in_count > 500 and list_out_count > 0:
            inner_list.append(address)
        else:
            pass
    return inner_list
        # yield row

def get_importance(staffs,myCollection_connection):
    '''
    获取一名员工的邮件重要程度的平均值
    '''
    importance_count = 0
    email_count = 0
    connection = myCollection_connection.find({'address':staffs['address']},{'connection':1})
    for cr in connection:
        for row in cr['connection']['in']['list']:
            importance_count += row['importance']
        for row in cr['connection']['out']['list']:
            importance_count += row['importance']
        email_count += cr['connection']['out']['count']
        email_count += cr['connection']['in']['count']
    avg_importance = importance_count/email_count
    return avg_importance
def main():
    repeat_dict = {}
    conn = MongoClient('127.0.0.1',27017)
    mydb = conn.test
    myColletion = mydb.email_data
    myCollection_list = mydb.list
    myCollection_inner = mydb.inner_staff
    myCollection_connection = mydb.connection
    print("读取数据库信息，获取内部员工邮件地址")
    data_reader = myCollection_list.find({'suffix':{'$regex':'hack','$options':'$i'}})
    inner_list = inner_staff(data_reader,myCollection_connection)
    print("正在处理员工信息，请稍候...")
    for staffs in inner_list:
        staff_commit = {}
        if staffs['name'] in repeat_dict.keys():
            pass
        else:
            repeat_dict[staffs['name']] = []
        #录入员工姓名
        staff_commit['name'] = staffs['name']

        #录入邮件地址
        staff_commit['address'] = []
        staff_commit['address'].append(staffs['address'])

        
        #录入员工的入职时间
        in_time = []
        time_reader = myCollection_connection.find({'address':{'$regex':staffs['name'],'$options':'$i'}},{'time':1})
        [in_time.append(row['time']) for row in time_reader ]
        staff_commit['in_time'] = min(in_time)

        #录入邮件重要程度，重要程度为平均值
        staff_commit['importance'] = get_importance(staffs,myCollection_connection)

        #联系人和邮件收发信息
        email_connection = get_releation(staffs,myCollection_connection)

        # 录入联系人信息
        releation = list(set(email_connection['list']))
        staff_commit['releation_people'] = releation

        #录入邮件收发数量
        staff_commit['in_count'] = email_connection['in']
        staff_commit['out_count'] = email_connection['out']
        
        #录入员工的邮件首发量
        staff_commit['starting'] = get_start(staffs['name'],myColletion)

        #将内部员工信息加入列表中
        repeat_dict[staffs['name']].append(staff_commit)
         
    print("邮件信息收集完毕")
    i = 0
    for keys in repeat_dict.keys():
        if len(repeat_dict[keys]) > 1:
            i+=1
            print('有多个邮箱的员工:'+keys)
            print("正在对员工邮箱信息进行整合...")  
            repeat_dict[keys] = reduce(integration,repeat_dict[keys])
        else:
            repeat_dict[keys] = repeat_dict[keys][0]
        if isinstance(repeat_dict[keys], dict):
            pass
        else:
            input()
    print("整合完成，已处理",i,"条邮箱信息")
    print("正在将数据存入数据库中")
    for keys in repeat_dict.keys():
        myCollection_inner.insert(repeat_dict[keys])
    print("处理完成")
if __name__ == '__main__':
    main()