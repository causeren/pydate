# coding:utf-8
# decription: test elasticsearch-py api
# create time: 20180611
# update time:
# update content:

from elasticsearch import Elasticsearch

"""
Connection method
"""
def connect_es(es_url):
    es = Elasticsearch(
        es_url,
        sniff_on_start=True,
        sniff_on_connection_fail=True,
        sniffer_timeout=600
    )
    return es

"""
Close method
"""

"""
GET method
"""
def es_query():



"""
PUT method
"""


"""
POST method
"""


"""
DELETE method
"""

"""
bulk method
"""

"""
mget method
"""

if __name__ = '__main__':
    es_host_cluster = ['masterp','slave124','slave125']
    try:
        connect_es(es_host_cluster)
    except:


"""
import json
from elasticsearch import Elasticsearch

hosts = []
es = Elasticsearch(hosts=hosts)

indices = ['indice0', 'indice1']

# Initialize the scroll
page = es.search(
    index=','.join(indices),
    doc_type='demo',
    scroll='2m',
    search_type='scan',
    size=1000,
    q='user_id:123 AND type:user'    # 填写 Kibana 搜索栏里的 Lucene 查询语法字符串
)
sid = page['_scroll_id']
scroll_size = page['hits']['total']
print 'total scroll_size: ', scroll_size

l = []
# Start scrolling
while scroll_size > 0:
    print "Scrolling..."
    page = es.scroll(scroll_id=sid, scroll='2m')
    # Update the scroll ID
    sid = page['_scroll_id']
    # Get the number of results that we returned in the last scroll
    scroll_size = len(page['hits']['hits'])
    print "scroll size: " + str(scroll_size)
    # Do something with the obtained page
    docs = page['hits']['hits']
    l += [x['_source'] for x in docs]

print 'total docs: ', len(l)

file_path = 'demo.json'
with open(file_path, 'wb') as f:
    json.dump(l, f, indent=2)
    
可以对比打印出来的doc数量与scroll size便可以检查是否全部记录都提取出来了。最后将数据存储到json文件中。

基于ES提供的python 客户端的方式可以提取的数量不要超过100万行，否则很容易超时失败。应该跟底层的http库有关系。

要从一个Index中提取超过千万行的数据，最佳实践是基于Java的客户端或者ES提供的Hadoop库，或者使用Python自己构造http请求，处理错误信息。

作者：大胡桃夹子
链接：https://www.jianshu.com/p/3c17561691a5
來源：简书
著作权归作者所有。商业转载请联系作者获得授权，非商业转载请注明出处。
"""