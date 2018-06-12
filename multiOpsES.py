#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
Python写入ES代码
https://www.jianshu.com/p/1f7db38db7cb
"""
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import time
import argparse
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

"""ES index and type
"""
INDEX_NAME = "twitter"
TYPE_NAME = "tweet"

# ES操作工具类
class es_tool():
    # initial
    def __init__(self, hosts, timeout):
        self.es = Elasticsearch(hosts, timeout=5000)
        pass

    # save data to es
    def set_data(self, fields_data=[], index_name=INDEX_NAME, doc_type=TYPE_NAME):
        # 创建ACTIONS
        ACTIONS = []
        # print "es set_data length", len(fields_data)
        for fields in fields_data:
            # print "fields", fields
            # print fields[1]
            action = {
                "_index": index_name,
                "_type": doc_type_name,
                "_source": {
                    "id": fields[0],
                    "tweet_id": fields[1],
                    "user_id": fields[2],
                    "user_screen_name": fields[3],
                    "tweet": fields[4]
                }
            }
            ACTIONS.append(action)

        # print "len ACTIONS", len(ACTIONS)
        # 批量处理
        success, _ = bulk(self.es, ACTIONS, index=index_name, raise_on_error=True)
        print('Performed %d actions' % success)

    # 读取参数
    def read_args(self):
        parser = argparse.ArgumentParser(description="Search Elastic Engine")
        parser.add_argument("-i", dest="input_file", action="store", help="input file1", required=False, default="./data.txt")
        return parser.parse_args()

    # 初始化es，设置mapping
    def init_es(hosts=[], timeout=5000, index_name=INDEX_NAME, doc_type_name=TYPE_NAME):
        es = Elasticsearch(hosts, timeout=5000)
        my_mapping = {
            TYPE_NAME: {
                "properties": {
                    "id": {
                        "type": "string",
                    },
                    "tweet_id": {
                        "type": "string"
                    },
                    "user_id": {
                        "type": "string"
                    },
                    "user_screen_name": {
                        "type": "string"
                    },
                    "tweet": {
                        "type": "string"
                    }
                }
            }
        }
        try:
            # 先销毁，后创建Index和mapping
            delete_index = es.indices.delete(index=index_name)
            create_index = es.indices.create(index=index_name)
            mapping_index = es.indices.put_mapping(index=index_name, doc_type=doc_type_name, body=my_mapping)

            if delete_index["acknowledged"] != True or create_index["acknowledged"] != True or mapping_index["acknowledged"] != True:
                print("Index creation failed...")
        except Exception as e:
            print('set_mapping except ',e)

if __name__ == '__main__':
    # args = read_args()
    # initial the es env
    init_es(hosts=["192.168.148.128:9200"], timeout=5000)
    # create es class
    es = es_tool(hosts=["192.168.148.128:9200"], timeout=5000)
    # write operation
    tweet_list = [("111", "222", "333", "444", "555"), ("11", "22", "33", "44", "55")]
    es.set_data(tweet_list)