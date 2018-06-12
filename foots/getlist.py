from pymongo  import MongoClient
from datetime import time
from re import compile

   
    
if __name__ == '__main__':
#连接test数据库并访问email_data集合
    conn = MongoClient('127.0.0.1',27017)
    mydb = conn.test
    myCollectlist = mydb.list
    myCollect = mydb.email_data
    address_list = []
    new_address_list = []
    i = 0
    data_reader = myCollect.find({},{'From(address)':1,'To(address)':1,'Cc(address)':1,'Bcc(address)':1},no_cursor_timeout=True)
    #将数据库独处的数据装入一个列表中，方便操作
    print("处理数据中...")
    for row in data_reader:
        address_list.append(row['From(address)'])
        address_list.append(row['To(address)'])
        address_list.append(row['Cc(address)'])
        address_list.append(row['Bcc(address)'])
    for items in address_list:
        if items != '' :
            [new_address_list.append(item_address) for item_address in items.split(";") if item_address != '' ]
        else:
            pass 
    new_address_list = list(set(new_address_list))

#拆分邮件地址并加入表list中
    print("正在加入到数据库中...")
    for address in new_address_list:
        i += 1
        email_address = address.split('@')
        address_name = email_address[0]
        address_suffix = email_address[1]
        myCollectlist.insert({
            'name' : address_name,
            'suffix' : address_suffix,
            'address' : address
            })
    print(i)