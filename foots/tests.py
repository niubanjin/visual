from gensim import corpora,models,similarities
from collections import defaultdict


#原始语料
raw_corpus = ["Human machine interface for lab abc computer applications",
             "A survey of user opinion of computer system response time",
             "The EPS user interface management system",
             "System and human system engineering testing of EPS",              
             "Relation of user perceived response time to error measurement",
             "The generation of random binary unordered trees",
             "The intersection graph of paths in trees",
             "Graph minors IV Widths of trees and well quasi ordering",
             "Graph minors A survey"]

#处理原始数据，产生的是一个列表
    # [['human', 'machine', 'interface', 'lab', 'abc', 'computer', 'applications'], 
    # ['survey', 'user', 'opinion', 'computer', 'system', 'response', 'time'], 
    # ['eps', 'user', 'interface', 'management', 'system'], 
    # ['system', 'human', 'system', 'engineering','testing', 'eps'], 
    # ['relation', 'user', 'perceived', 'response', 'time', 'error', 'measurement'], 
    # ['generation', 'random', 'binary','unordered', 'trees'], 
    #  ['intersection', 'graph', 'paths', 'trees'], 
    #  ['graph', 'minors', 'iv', 'widths', 'trees', 'well', 'quasi', 'ordering'], 
    #  ['graph', 'minors', 'survey']]
stoplist = set('for a of the and to in'.split(' '))
texts = [[word for word in document.lower().split() if word not in stoplist]
          for document in raw_corpus]

#设置值的默认类型为int,默认为0           
frequency = defaultdict(int)

#将值加入到 frequency集合中
for text in texts:
    for token in text:
        frequency[token] += 1

#返回主题中出现次数超过1次的主题
precessed_corpus = [[token for token in text if frequency[token] > 1] for text in texts]

#将主题列表转换为字典
dictionary = corpora.Dictionary(precessed_corpus)
#将新增的语料转化为向量
# new_doc = "human computer interaction"
# new_vec = dictionary.doc2bow(new_doc.lower().split())

#将原始的语料转化为词袋向量
bow_corpus = [dictionary.doc2bow(text) for text in precessed_corpus]

#将向量（原始语料转化的向量）转化为TF-IDF模型,并使用新的语料进行训练
tfidf = models.TfidfModel(bow_corpus)
# string = "system minors"
# string_bow = dictionary.doc2bow(string.lower().split())
string_tfidf = tfidf[bow_corpus]

#将 TF-IDF转化为lda模型
lda = models.LdaModel(string_tfidf, id2word = dictionary, num_topics = 6)   
lda_topics = lda.print_topics()
#测试文档的lda结果
test_doc = ['human', 'machine', 'interface', 'lab', 'abc', 'computer', 'applications','system', 'human', 'system', 'engineering','testing', 'eps','graph', 'minors', 'iv', 'widths', 'trees', 'well', 'quasi', 'ordering']
doc_bow = dictionary.doc2bow(test_doc)
doc_lda = lda[doc_bow]

for topic in lda_topics:
     print(topic)
print(doc_lda)