# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy import signals
import json
import codecs
import os

class JsonWithEncodingNovelPipeline(object):

    def __init__(self):
        self.file = codecs.open('novel.json', 'a', encoding='utf-8')

    def process_item(self, item, spider):
        if item is not None:
            line = json.dumps(dict(item), ensure_ascii=False) + "\n"
            if item.has_key('author'):
                self.file.write(line)
            elif item.has_key('chapterId'):
                if not os.path.isdir(item['bookId']):
                    os.mkdir(item['bookId'])
                if not os.path.exists(item['bookId'] + '/' + item['chapterId']):
                    file2 = codecs.open(item['bookId'] + '/' + item['chapterId'], 'a', encoding ='utf-8')
                    file2.write(line)
        return item

    def spider_closed(self, spider):
        self.file.close()

