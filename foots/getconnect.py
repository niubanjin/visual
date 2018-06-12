from pymongo  import MongoClient
from datetime import time
from method import timetostr
from re import compile
import time as t


def main(power):
    t.clock()
    conn = MongoClient('127.0.0.1',27017)
    mydb = conn.test
    myCollectlist = mydb.list
    myCollection = mydb.email_data
    if power == "hour":
        mycollectconnection = mydb.hour_connetion
    if power == "day":
        mycollectconnection = mydb.connection
    if power == "month":
        mycollectconnection = mydb.month_connection
    if power == "year":
        mycollectconnection = mydb.year_connection
    email_lists = []
    email_dict = {}
    name_dict = {}
    i = 0
    j = 0
    
    data_readerlist = myCollectlist.find({})
    data_reader = myCollection.find({},no_cursor_timeout = True).limit(200000)
    #遍历data_reader将数据以需要的格式存到email_dict中
    print("处理邮件地址中...")
    for row in data_reader:   
        temp = '{0};{1};{2}'.format(row['To(address)'],row['Bcc(address)'],row['Cc(address)']) 
        if temp != ';;':
            to_list = [item for item in temp.split(';') if item != '' and '@' in item]
            importance = row['Importance'] if row['Importance'] !='' else 1
            # recevid = t.strftime("%Y-%m-%d %H:%M:%S",tuple(row['Date Received']))
            email_dict = {
                'From':row['From(address)'],
                'To': to_list,
                'Importance' : importance,
                'Subject' : row['Subject'],
                'Date Received': row['Date Received'],
                'Date Sent': timetostr(row['Date Sent'],power)
            }
            email_lists.append(email_dict)
        else:
            pass
    print(len(email_lists))
    #读取地址列表
    for row in data_readerlist:
        keys = row['address']
        name_dict[keys] = {}
    # 统计出度
    for row in email_lists:
        if row['From'] !='' and row['Date Sent'] != None and '@' in row['From']:
            keys = row['From'] 
            print("正在统计",row['From'],"的出度")
            try:
                starttime = row['Date Sent']
                # length = len(row['To'])
                out_number = name_dict[keys][starttime]['out']['count'] 
            except KeyError:        
                name_dict[keys][starttime] = {
                    'in' : {
                        'count' : 0,
                        'list' : []
                    },
                    'out' : {
                        'count' : 1,
                        'list' : [{
                            'to_address' : row['To'],
                            'subject' : row['Subject'],
                            'importance' : row['Importance']
                        }]
                    },
                }  
            else:
                name_dict[keys][starttime]['out']['count'] += 1
                name_dict[keys][starttime]['out']['list'].append({
                    'to_address' : row['To'],
                    'subject' : row['Subject'],
                    'importance' : row['Importance']
                })
        else:
            pass
    # 统计入度
    for toaddress in email_lists:
        starttime = toaddress['Date Sent']
        if toaddress['From'] !='' and toaddress['Date Sent'] != None and '@' in toaddress['From'] :
            for row in toaddress['To']:
                print("正在统计",row,"的入度")
                keys = row 
                try:
                    in_number = name_dict[keys][starttime]['in']['count'] 
                except KeyError:
                    try:        
                        name_dict[keys][starttime] = {
                            'in' : {
                                'count' : 1,
                                'list' : [{
                                    'from_address' : toaddress['From'],
                                    'subject' : toaddress['Subject'],
                                    'importance' : toaddress['Importance'],
                                    'received' : toaddress['Date Received']
                                }]
                            },
                            'out' : {
                                'count' : 0,
                                'list' : []
                            },
                        } 
                    except TypeError as e:
                        print(row)
                        print(e)
                        input()
                else:
                    name_dict[keys][starttime]['in']['count'] += 1
                    name_dict[keys][starttime]['in']['list'].append({
                                'from_address' : toaddress['From'],
                                'subject' : toaddress['Subject'],
                                'importance' : toaddress['Importance'],
                                'received' : toaddress['Date Received']
                            })
        else:
            pass

    print("正在插入数据库中")
    for name_keys in name_dict.keys():
        for time_keys in name_dict[name_keys].keys():
            i+=1
            mycollectconnection.insert({
                'address' : name_keys,
                'time' : time_keys,
                'connection' : name_dict[name_keys][time_keys]
            })
    print("处理完成，已处理",i,"条数据")
if __name__ =="__main__":
    main("hour")