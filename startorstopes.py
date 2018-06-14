#!/usr/bin/python36
# -*- coding:utf-8 -*-

"""The script to start or stop the es cluster.
"""

import os
import sys
import imp
import time

imp.reload(sys)
#print(sys.getdefaultencoding())


def start_es():
        try:
                os.system('/opt/elasticsearch1/bin/elasticsearch -d -Des.insecure.allow.root=true')
                time.sleep(2)
        except Exception as e:
                print("ES node1 start error, raise exception: " + str(e))
                sys.exit()
        try:
                os.system('/opt/elasticsearch2/bin/elasticsearch -d -Des.insecure.allow.root=true')
                time.sleep(2)
        except Exception as e:
                print("ES node1 start error, raise exception: " + str(e))
                sys.exit()
        try:
                os.system('/opt/elasticsearch3/bin/elasticsearch -d -Des.insecure.allow.root=true')
                time.sleep(2)
        except Exception as e:
                print("ES node1 start error, raise exception: " + str(e))
                sys.exit()
        time.sleep(3)
        jpsout = os.popen('jps', 'r').read()
        esnodes = 0
        for line in jpsout.split('\n'):
                if line.find("Elasticsearch"): esnodes +=1
        print("succes start the ES nodes: " + str(esnodes))

def stop_es():
        jpsout = os.popen('jps', 'r').read()
        #print(jpsout)
        lines = jpsout.split('\n')
        for line in lines:
                #print(line)
                id_and_name = line.split(' ')
                #print(id_and_name[0])
                if len(id_and_name) > 1:
                        if id_and_name[1] == "Elasticsearch":
                                try:
                                        killstr = "kill -9 " + id_and_name[0]
                                        #print(killstr)
                                        os.system(killstr)
                                except Exception as e:
                                        print("count not kill pid: " + id_and_name[0] + "\n" + "raise Exception: " + str(e))
        time.sleep(6)
        jpsout = os.popen('jps', 'r').read()
        for line in jpsout.split('\n'):
                if line.find("Elasticsearch"): print("something got wrong.please kill es manually...")
                sys.exit()
        print("success stop the es cluster!")

if __name__ == '__main__':
        if len(sys.argv) < 1:
                print('at least input a parameter')
                sys.exit()
        tasktype = sys.argv[1]
        if tasktype == "start":
                start_es()
        elif tasktype == "stop":
                stop_es()
        else:
                print("unknow tasktype!!!")
                sys.exit()
