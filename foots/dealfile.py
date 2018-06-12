
from time import strptime
from pymongo   import MongoClient
from time import strptime
from os import  listdir
from os import  path 
from re import compile
from method import compare_dict
from method import parsename

def revised(address):
    address = address.split(';')
    email_address = []
    for row in address:
        if row != '':
            str_address = row.split('@')
            if str_address[1] =="hackingteam":
                str_name = str_address[0].split(' ')
                if str_name.__len__() > 1:
                    name = "{0}.{1}".format(str_name[0][0],str_name[1])
                    new_address = "{0}@{1}".format(name,str_address[1])
                    email_address.append(new_address)
                else:
                    email_address.append(row)
            else:
                email_address.append(row.lower())
        else:
            pass
    return email_address


def parsesmtp(address):
    new_address_list = []
    address = address.split(';')
    for row in address:
        if row != '':
            if '=' in row:
                list4 = row.split('=')
                index = len(list4)-1
                str_suffix = list4[1]
                str_suffixs = str_suffix.split('/')
                str_suffix = str(str_suffixs[0]).lower()
                row = str(list4[index]).lower()
                regex = compile("[0-9]")
                number = regex.findall(row)
                if len(number) == 0:
                    email_address = "{0}@{1}".format(row,str_suffix)
                else:
                    index = row.index(number[0])
                    row = row[:index] 
                    email_address = "{0}@{1}".format(row,str_suffix) 
            else:
                email_address = row
            new_address_list.append(email_address)
        else:
            pass
    return new_address_list
if __name__ == '__main__':
    i = 0
    conn = MongoClient('127.0.0.1',27017)
    mydb = conn.test
    address_list = []
    myCollect = mydb.email_data
    print("读取数据库中...")
    data_reader=myCollect.find({},{"From(display)":1,"Creator Name":1})
    i = 0
    j = 0
    # for row in data_reader:
    #     try:
    #         print("正在处理",row)
    #         i+=1
    #         if row['Date Sent'] != '' and '/' in row['Date Sent'] :
    #             row['Date Sent'] = row['Date Sent'].replace('/','-')
    #         if row['Date Received'] != '' and '/' in row['Date Received']:
    #             row['Date Received'] = row['Date Received'].replace('/','-')
    #         if isinstance(row['Date Sent'],str) ==True:
    #             row['Date Sent'] = strptime(row['Date Sent'],'%Y-%m-%d %X')
    #         if isinstance(row['Date Received'],str) ==True:
    #             row['Date Received'] = strptime(row['Date Received'],'%Y-%m-%d %X')
    #         if row['Importance'] != '':
    #             row['Importance'] = int(row['Importance']) 
    #         else:
    #             row['Importance'] = 1
    #         myCollect.update({'_id':row['_id']},{'$set':{'Date Sent':row['Date Sent'],'Date Received':row['Date Received'],'Importance':row['Importance']}})
    #     except ValueError:  
    #         j+=1
    #         myCollect.remove({'_id':row['_id']})
    # print("完成")
    # print("exception出现",j,"次")
# # 将所有邮件地址处理为需要的格式，主要是针对由于邮件协议造成的不同邮件格式统一起来
    print("处理数据中...")
    # for row in data_reader:
    #     address_dict = dict([('_id',row['_id']),('From(address)',row['From(address)']),('To(address)',row['To(address)']),('Bcc(address)',row['Bcc(address)']),('Cc(address)',row['Cc(address)'])])
    #     address_list.append(address_dict)
    # for address_dict in address_list:
    #     try:
    #         i += 1
    #         for keys in address_dict.keys():
    #             if keys != '_id':
    #                 if address_dict[keys] != '':
    #                     new_address = revised(address_dict[keys])
    #                     address_dict[keys] = ';'.join(new_address)
                        
    #                 else:
    #                     pass
    #     except IndexError as e:
    #         myCollect.remove({'_id':address_dict['_id']})
    #         j += 1
    #先执行这一步
    # print("处理数据中...")
    # try:
    #     for address_dict in address_list:
    #         for keys in address_dict.keys():
    #             if keys != '_id':
    #                 if  '=' in address_dict[keys]:
    #                     new_address = parsesmtp(address_dict[keys])
    #                     address_dict[keys] = ';'.join(new_address)
    #                 else :
    #                     address_dict[keys] = address_dict[keys].lower()
    #             else:
    #                 pass
    # except IndexError as e:
    #     print(e)
    #     input()
    # print("正在加入数据库中")
    # for row in address_list:
    #     i +=1
    #     myCollect.update({"_id":row["_id"]},{'$set':{
    #         "From(address)" : row["From(address)"],
    #         "To(address)" : row["To(address)"],
    #         "Bcc(address)" : row["Bcc(address)"],
    #         "Cc(address)" : row["Cc(address)"]                     
    #     }
    #     })
    for row in data_reader:
        try:
            row["Creator Name"] = parsename(row["Creator Name"])
            row["From(display)"] = parsename(row["From(display)"])
            i+=1
            print(row["From(display)"])
            print(row["Creator Name"])
        except IndexError as e:
            print("from",row["From(display)"])
            print("creator",row["Creator Name"])
            print(e)
            input()
        myCollect.update({
            "_id":row["_id"]
            },
            {
                "$set":{
                    "From(display)" : row["From(display)"],
                    "Creator Name": row["Creator Name"] 
                }    
            }
        )
    print("已处理",i,"条数据")
    # print("异常数据",j)



