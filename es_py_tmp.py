# -*- coding: utf-8 -*-
# base_on: https://blog.csdn.net/y472360651/article/details/76652021

"""search all doc
"""
es.search(index="my_index", doc_type="test_type")

# or
body = {
    "query": {
        "match_all": {}
    }
}
es.search(index="my_index", doc_type="test_type", body=body)

"""term and terms
"""
# term
body = {
    "query": {
        "term": {
            "name": "python"
        }
    }
}
# search data which's name is "python"
es.search(index="my_index", doc_type="test_type", body=body)

# terms
body = {
    "query": {
        "terms": {
            "name": ["python", "android"]
        }
    }
}

# search data which name is "python" or "android"
es.search(index="my_index", doc_type="test_type", body=body)

"""match and multi_match
"""
# match: matching data which's name contain the key word:python
body = {
    "query": {
        "match": {
            "name": "python"
        }
    }
}

# search data which's name contain the key word: python
es.search(index="my_index", doc_type="test_type", body=body)

# multi_match: matching more than two fields like "name" and "addr" which contain key word：深圳
body = {
    "query": {
        "multi_match": {
            "query": "深圳",
            "fields": ["name", "addr"]
        }
    }
}

# search data multi fields which contain key word:
es.search(index="my_index", doc_type="test_type", body=body)

# ids
body = {
    "query": {
        "ids": {
            "type": "test_type",
            "values": ["1", "2"]
        }
    }
}

# search data field id which value is 1 or 2
es.search(index="my_index", doc_type="test_type", body=body)

"""复合查询bool
"""
# bool有3类查询关系, must(都满足），should（其中一个满足）， must_not(都不满足）
body = {
    "query": {
        "bool": {
            must: [
                {
                    "term": {"name": "python"}
                },
                {
                    "terms": {"age": 18}
                }
            ]
        }
    }
}
# search data fields name and age which value is "python" and 18
es.search(index="my_index", doc_type="test_type", body=body)

"""切片式查询
"""
body = {
    "query": {
        "match_all": {}
    }
    "from": 2   # 从第二条数据开始
    "size": 4   # 获取4条数据
}
# search data from index 2 and offset 4
es.search(index="my_index", doc_type="test_type", body=body)

"""范围查询
"""
body={
    "query": {
        "range": {
            "age": {
                "gte": 18,  # >=18
                "lte": 30,  # <=30
            }
        }
    }
}
# search data field age which value is between 18 and 30
es.search(index="my_index", doc_type="test_type", body=body)

"""前缀查询
"""
body = {
    "query": {
        "prefix": {
            "name": "p"
        }
    }
}
# search data field name which value prefix is "p"
es.search(index="my_index", doc_type="test_type", body=body)

"""通配符查询
"""
body = {
    "query": {
        "wildcard": {
            "name": "*id"
        }
    }
}
# search data field name which value contain "id"
es.search(index="my_index", doc_type="test_type", body=body)

"""排序
"""
body = {
    "query": {
        "match_all": {}
    }
    "sort": {
        "age": {            # 根据age字段升序排序
            "order": "asc"  # asc升序，desc降序
        }
    }
}
es.search(index="my_index", doc_type="test_type", body=body)

"""filter_path
"""
# 响应过滤--response filter
# 只需要获取_id数据，多个条件用逗号隔开
es.search(index="my_index", doc_type="test_type", filter_path=["hits.hits._id"])

# 获取所有数据
es.search(index="my_index", doc_type="test_type", filter_path=["hits.hits._*"])

"""count
"""
# 执行查询并获取该查询的匹配数
# 获取数据量
es.count(index="my_index", doc_type="test_type")

"""度量类聚合
"""
# - 获取最小值
body = {
    "query": {
        "match_all": {}
    }
    "aggs": {                   # 聚合查询
        "min_age": {            # 最小值的key
            "min": {            # 最小
                "field": "age"  # 查询"age"的最小值
            }
        }
    }
}

# 搜索所有数据，并获取age最小值
es.search(index="my_index", doc_type="test_type", body=body)

# - 获取最大值
body = {
    "query": {
        "match_all": {}
    }
    "aggs": {                   # 聚合查询
        "max_age": {            # 最大值的key
            "max": {            # 最大
                "field": "age"  # 查询"age"的最大值
            }
        }
    }
}
es.search(index="my_index", doc_type="test_type", body=body)

# - 获取和
body = {
    "query": {
        "match_all": {}
    }
    "aggs": {                   # 聚合查询
        "sum_age": {            # 和的key
            "sum": {
                "field": "age"
            }
        }
    }
}

# - 获取平均值
body = {
    "query": {
        "match_all": {}
    }
    "aggs": {                   # 聚合查询
        "avg_age": {            # 平均值的key
            "avg_age": {
                "avg": {
                    "field": "age"
                }
            }
        }
    }
}