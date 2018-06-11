# coding:utf-8
# decription: a client to operate elasticsearch
# create time: 20180611
# update time:
# update content:

"""
Elasticsearch Clients:https://www.elastic.co/guide/en/elasticsearch/client/index.html
    Java REST Client[6.2]
    Java API[6.2]
    JavaScript API
    Groovy API[2.4]
    .NET API[6.x]
    PHP API[6.0]
    Perl API
    Python API
    Ruby API
    Community Contributed Clients
elasticsearch-py:
Overview: https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/index.html
Full Documentation: http://elasticsearch-py.readthedocs.io/en/master/
Github: https://github.com/elastic/elasticsearch-py
"""

from elasticsearch import Elasticsearch
from elasticsearch import helpers
import datetime


"""
Elasticsearch
class elsticsearch.Elasticsearch(hosts=None,transport_class=<class 'elasticsearch.transport.Transport'>, **kwagrs)
    Parameters:
    - hosts: list of nodes we should connect to. Node should be a dictionary({"host":"localhost","port":9200}),
             the entire dictionary will be passed to the Connection class as kwargs, or a string in the format 
             of host[:port] which will be translated to a dictionary automatically. If no value is given the 
             Urllib3HttpConnection class defaults will be used.
    - transport_class: Transport subclass to use.
    - kwargs: any additional arguments will be passed on to the Transport class and,subsequently,to the Connection instances.

"""

class OpsES(object):
    def __init__(self):
        pass


    @classmethod
    def connect_host(cls):
        """
        hosts = [{"host": "masterp123","port": 9200},
                 {"host": "slave124","port": 9200},
                 {"host": "slave125","port": 9200}]
        """
        hosts = ["masterp123","slave124","slave125"]
        """
        # create connection that will automatically inspect the cluster to get 
        # the list of active nodes. Start with nodes running on 'esnode1' and 
        # 'esnode2'
        es = Elasticsearch(
            ['esnode1', 'esnode2'],
            # sniff before doing anything
            sniff_on_start=True,
            # refresh nodes after a node fails to respond
            sniff_on_connection_fail=True,
            # and also every 60 seconds
            sniffer_timeout=60
        )
        # Different hosts can have different parameters, use a dictionary per node to specify those:
        # connect to localhost directly and another node using SSL on port 443
        # and an url_prefix. Note that ``port`` needs to be an int.
        es = Elasticsearch([
            {'host': 'localhost'},
            {'host': 'othernode', 'port': 443, 'url_prefix': 'es', 'use_ssl': True},
        ])
        # If using SSL,there are several parameters that control how we deal with certificates(see Urllib3HttpConnection for detailed description of the options):
        es = Elasticsearch(
            ['localhost:443', 'other_host:443'],
            # turn on SSL
            use_ssl=True,
            # make sure we verify SSL certificates(off by default)
            verify_certs=True,
            # provide a path to CA certs on disk
            ca_certs='/path/to/CA_certs'
        )
        # SSL client authentication is supported(see Urllib3HttpConnection for detailed description of the options):
        es = Elasticsearch(
            ['localhost:443', 'other_host:443'],
            # turn on SSL,
            use_ssl=True,
            # make sure we verify SSL certificates(off by default)
            verify_certs=True,
            # provide a path to CA certs on disk
            ca_certs='/path/to/CA_certs',
            # PEM formatted SSL client certificate
            client_cert='/path/to/clientcert.pem',
            # PEM formatted SSL client key
            client_key='/path/to/clientkey.pem'
        )
        # Alternatively you can use RFC-1738 formatted URLs, as long as they are not in conflict with other options:
        es = Elasticsearch(
            [
                'http://user:secret@localhost:9200/',
                'https://user:secret@other_host:443/production'
            ],
            verify_certs=True
        )
        """

        es = Elasticsearch(
            hosts,
            sniff_on_start=True,
            sniff_on_connection_fail=True,
            sniffer_timeout=600
        )
        return es

    """
    
    """
    def es_query(self, domain="", start=None, end=None, reverse=False, limit_cnt=20, category=0):
        es = OpsES.connect_host()
        now = datetime.datetime.now()
        if reverse:
            order = "desc"
        else:
            order = "asc"
        if not start:
            start = now - datetime.timedelta(weeks=2000)
        if not end:
            end = now
        range_body = {
            "range": {
                "time": {
                    "gte": start,
                    "lte": end
                }
            }
        }
        and_list = [range_body]
        domain_body = {
            "term": {
                "domain": domain
            }
        }
        category_body = {
            "term": {
                "category": category
            }
        }
        if domain:
            and_list.append(domain_body)
        if category:
            and_list.append(category_body)
        q_body = {
            "size": limit_cnt,
            "sort": [
                {
                    "time": {
                        "order": order
                    }
                }
            ],
            "query": {
                "filtered": {
                    "query": {"matchAll": {}},
                    "filter": {
                        "and": and_list
                    }
                }
            }
        }
        res = es.search(body=q_body)
        ret = []
        for hit in res["hits"]["hits"]:
            value = {}
            src = hit["_source"]
            if src:
                try:
                    the_time = src["time"]
                    if len(the_time) < 20:
                        value["time"] = datetime.datetime.strptime(the_time, "%Y-%m-%dT%H:%M:%S")
                    else:
                        value["time"] = datetime.datetime.strptime(the_time, "%Y-%m-%dT%H:%M:%S.%f")
                    ret.append(value)
                except Exception as e:
                    print
                    str(e)
                    ret = []
                    print
                    "Query xxxxx data failed!"
        return ret

    def es_bulk(self):
        es = self.connect_host()
        es.bulk(index="hello", )