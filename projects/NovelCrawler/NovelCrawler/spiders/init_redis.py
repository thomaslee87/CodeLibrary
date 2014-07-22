#!/usr/bin/python

import sys
import os
import json
import redis

PRE_REDIS_KEY = 'bk_'

DIR = '/home/lizheng/work/NovelCrawler/NovelCrawler/spiders'
redis = redis.Redis('127.0.0.1',6379)

fd = open(DIR + '/books.json', 'r')
books = [json.loads(line.strip()) for line in fd.readlines()]
fd.close()

for book in books:
    book_id = book['id']
    redis.set(PRE_REDIS_KEY + book_id, book)
