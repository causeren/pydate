#!/usr/bin/env python
# -*- coding:utf-8 -*-

from datetime import datetime
import threading
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from openpyxl import load_workbook
import sys
import time
from multiprocessing import Process

reload(sys)
sys.getdefaultencoding('utf-8')

def