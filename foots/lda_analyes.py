from gensim import corpora,models,similarities
from collections import defaultdict
from pymongo import MongoClient
from re import compile
def get_corpus(data_reader):
    corpus_list = []
    [corpus_list.append(row['Subject']) for row in data_reader if row != '']

def deal_corpus(subjects):
    temp_list = subjects.split(':')
    index = len(temp_list) - 1
    sybject = temp_list[index]

def main():
    conn = MongoClient('127.0.0.1',27017)
    mydb = conn.test
    myCollection = mydb.email_data
    data_reader = myCollection.find({},{'Subject':1})
    get_corpus(data_reader)


if __name__ == '__main__':
    main()