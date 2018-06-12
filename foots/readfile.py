from pymongo import MongoClient 
from csv  import  DictReader
from time import strptime
from os import  listdir
from os import  path 
from datetime import time


if __name__=="__main__":
    path1="/home/kobe/File/Gradutation/datas/m.romeo.csv"
    #build connection  
    conn = MongoClient('127.0.0.1',27017)  
    #connect dataBase  
    mydb = conn.test  
    #get collection  
    myCollect = mydb.email_data
    #get data
    i = 0
    with open(path1,encoding='UTF-8') as f:
        csv_reader = DictReader((line.replace('\0','') for line in f),delimiter=",")       
        for row in csv_reader: 
            i=i+1          
            # insert data to mongodb
            myCollect.insert({
            'Subject':row['ubject'],
            'Creator Name':row['Creator Name'],
            'Importance':int(row['Importance']),
            'Bcc(display)':row['Bcc (display)'],
            'Bcc(address)':row['Bcc (address)'],
            'Size':row['Size'],                               
            'Cc(display)':row['Cc (display)'],
            'Cc(address)':row['Cc (address)'],                           
            'Date Sent':row['Date Sent'],
            'Date Received':row['Date Received'].replace('/','-'),
            'From(display)':row['From (display)'].replace('/','-'),
            'From(address)':row['From (address)'],
            'To(display)':row['To (display)'],
            'To(address)':row['To (address)'] 
            # 'out_degree':
            # 'in_degree':
            }
                 )  
    print(i)
    print("it,s ok!")  


