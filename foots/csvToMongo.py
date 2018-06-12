from time import strptime
import os
from csv import DictReader
from pymongo   import MongoClient
from datetime import time
from itertools import islice

def findfile(work_dir):
    files = []
    for parent, dirnames, filenames in os.walk(work_dir):
        for filename in filenames:
            file_path = os.path.join(parent, filename)
            files.append(file_path)
    return files 
    
if __name__=="__main__":
    pathes=r"/media/kobe/7bdf7e57-b650-4eca-aeda-64f541d1ea42/files/File/Gradutation/datas"
    #build connection  
    conn = MongoClient('127.0.0.1',27017)  
    #connect dataBase  
    mydb = conn.test  
    #get collection  
    myCollect = mydb.email_data
    #get file
    files = findfile(pathes)
    #get data
    i = 0
    j=0
    for file1 in files:
        print(file1)
        with open(file1) as f:
            if file1 ==r"/media/kobe/7bdf7e57-b650-4eca-aeda-64f541d1ea42/files/File/Gradutation/datas/m.romeo.csv":
                csv_reader= DictReader((line.replace('\0','') for line in f),delimiter=",")
                for row in csv_reader:    
                    i=i+1 
                    try:      
                        #insert data to mongodb
                        myCollect.insert({
                            'Subject':row['ubject'],
                            'Creator Name':row['Creator Name'],
                            'Importance':row['Importance'],
                            'Bcc(display)':row['Bcc (display)'],
                            'Bcc(address)':row['Bcc (address)'],
                            'Size':row['Size'],                               
                            'Cc(display)':row['Cc'],
                            'Cc(address)':row['Cc (address)'],                           
                            'Date Sent': row['Date Sent'],
                            'Date Received': row['Date Received'],
                            'From(display)':row['From (display)'],
                            'From(address)':row['From (address)'],
                            'To(display)':row['To (display)'],
                            'To(address)':row['To (address)'] 
                        }
                            )  
                    except Exception as e:
                        print(row)
                        print(e)
                        input()
                        j = j+1
            elif  file1 ==r"/media/kobe/7bdf7e57-b650-4eca-aeda-64f541d1ea42/files/File/Gradutation/datas/.~capaldo.csv":
                i+=1
                myCollect.insert({
                            'Subject':row['Subject'],
                            'Creator Name':row['Creator Name'],
                            'Importance':row['Importance'],
                            'Bcc(display)':row['Bcc (display)'],
                            'Bcc(address)':row['Bcc (address)'],
                            'Size':row['Size'],                               
                            'Cc(display)':row['Cc (display)'],
                            'Cc(address)':row['Cc (address)'],                           
                            'Date Sent': row['Date Sent'],
                            'Date Received': row['Date Received'],
                            'From(display)':row['From (display)'],
                            'From(address)':row['From (address)'],
                            'To(display)':row['To (display)'],
                            'To(address)':row['To (address)'] 
                        }
                            )  
            else:
                csv_reader = DictReader(f)  
                k = 0     
                for row in csv_reader:    
                    i=i+1  
                    k+=1    
                    try:      
                        #insert data to mongodb
                        myCollect.insert({
                            'Subject':row['Subject'],
                            'Creator Name':row['Creator Name'],
                            'Importance':row['Importance'],
                            'Bcc(display)':row['Bcc (display)'],
                            'Bcc(address)':row['Bcc (address)'],                            
                            'Cc(display)':row['Cc (display)'],
                            'Cc(address)':row['Cc (address)'],                           
                            'Date Sent': row['Date Sent'],
                            'Date Received': row['Date Received'],
                            'From(display)':row['From (display)'],
                            'From(address)':row['From (address)'],
                            'To(display)':row['To (display)'],
                            'To(address)':row['To (address)'] 
                        }
                            )  
                    except Exception as e:
                        print(row)
                        print(k)
                        print(e)
                        input()
                        j = j+1
    print("已载入",i,"条数据")
    print("失败",j,"条数据")


