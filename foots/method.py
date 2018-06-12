from os import  listdir
from os import  path 
from time import strptime
from pymongo  import MongoClient
from re import compile
import time as t

def findfile(pathes):
    """
    这是一个寻找指定目录下文件的方法，参数是路径，
    返回值是一个文件列表，但是只包含文件名。
    """
    files=listdir(pathes)
    s=".csv"
    for file1 in files:
        suffix = path.splitext(file1)
        suffix = suffix[1]  
        if suffix != s:
            files.remove(file1)
    return files  

def compare_dict(dict1,dict2,key_list):
    """
    这是一个比较字典大小的方法，参数是字典和键的列表。成功则返回True,否则返回False
    """
    flag=True
    keys1=dict1.keys()
    keys2=dict2.keys()
    if key_list.length !=0:
        for key in key_list:
            if key in keys1 and key in keys2:
                if dict1[key]==dict2[key]:
                    flag=flag & True
                else:
                    flag=flag & False
            else:
                raise Exception(str.format("the {0} not in key_list",key))
    else:
        raise Exception("key_list's length is zero!")
    return flag
   
def getnum(row):
    '''
    获取num
    '''
    return row["value"]
def parsename(str_name):
    if str_name != "":
        str_name = str_name.strip()
        str_name = str_name.lower()
        str1 = str_name.split(" ")
        if len(str1) > 1:
            str2 = str1[0][0]
            new_name = "{0}.{1}".format(str2,str1[1])
        else:
            new_name = str_name
    else:
        new_name = "" 
    return new_name
def parsetext(str_name):
    if str_name == "importance":
        return "邮件平均重要度"
    if str_name == "in_count":
        return "收件数量"
    if str_name == "out_count":
        return "发送邮件数量"
    if str_name == "releation_people":
        return "联系人列表"
    if str_name == "starting":
        return "邮件首发量"
    return str_name

def timetostr(list1,power):
    '''
    将时间元祖转化为字符串，参数是元祖list1和统计粒度power
    '''
    if len(list1) != 0:
        time_tuple = tuple(list1)
        if power == 'day':   
            strtime = t.strftime("%Y-%m-%d", time_tuple)
        if power == 'hour':
            strtime = t.strftime("%Y-%m-%d %H", time_tuple)
        if power == 'month':
            strtime = t.strftime("%Y-%m", time_tuple)
        if power == 'year':
            strtime = t.strftime("%Y",time_tuple)
        return strtime
    else:
        return None
def subject_time(list1,power):
    '''
    将时间元祖转化为字符串，参数是元祖list1和统计粒度power
    '''
    if len(list1) != 0:
        time_tuple = tuple(list1)
        if power == 'year':   
            strtime = t.strftime("%Y", time_tuple)
        # if power == 'week':
        #     strtime = t.strftime("%Y-%m-%d %H", time_tuple)
        if power == 'month':
            strtime = t.strftime("%Y-%m", time_tuple)
        return strtime
    else:
        return None
def parse_subject(old_subject):
    '''
    对主题进行分析
    '''
    #去除多余字符串
    temp = old_subject.split(":")
    index = temp.__len__() - 1  
    subject = temp[index]
    temp = subject.split("]")
    #去除标头
    index = temp.__len__() - 1
    new_subject = temp[index] 
    new_subject = new_subject.strip()
    return new_subject



    







                        
