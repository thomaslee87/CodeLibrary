#-*-encoding:utf-8-*-
#!/usr/bin/python
import re
import json
import datetime
import redis

from scrapy.selector import Selector
try:
    from scrapy.spider import Spider
except:
    from scrapy.spider import BaseSpider as Spider
from scrapy.utils.response import get_base_url
from scrapy.utils.url import urljoin_rfc
from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor as sle

from NovelCrawler.items import *
from NovelCrawler.misc.log import *


class NovelSpider(CrawlSpider):
    name = "Novel"
#    download_delay = 2
    allowed_domains = ["www.23us.com"]
    start_urls = [
#        "http://www.23us.com/",
#        "http://www.23us.com/class/1_1.html",
#        "http://www.23us.com/class/2_1.html",
#        "http://www.23us.com/class/3_1.html",
#        "http://www.23us.com/class/4_1.html",
#        "http://www.23us.com/class/5_1.html",
#        "http://www.23us.com/class/6_1.html",
#        "http://www.23us.com/class/7_1.html",
#        "http://www.23us.com/class/8_1.html",
#        "http://www.23us.com/class/9_1.html",
#        "http://www.23us.com/class/10_1.html",
#        "http://www.23us.com/quanben/1"
        'http://www.23us.com/top/allvisit_1.html',
        'http://www.23us.com/top/monthvisit_1.html'
    ]

    cache = redis.Redis('127.0.0.1',6379)

    rules = [
        Rule(sle(allow=("/book/\\d+$")), follow=True, callback='parse_book'),
        Rule(sle(allow=("/html/\\d+/\\d+/\\d+\\.html$")), callback='parse_chapter'),
        Rule(sle(allow=("/html/\\d+/\\d+/$")))
#        Rule(sle(allow=("/top/allvisit_\d.html$")), follow=True, callback='parse_topall')
    ]

    def parse_topall(self,response):
        sel = Selector(response)
        base_url = get_base_url(response)
        

    def parse_chapter(self, response):
#        dt = datetime.datetime.strptime(response.headers['Last-Modified'], '%a, %d %b %Y %H:%M:%S GMT' )
#        if dt.strftime('%Y-%m-%d') < '2014-07-17':
#            return [];
        sel= Selector(response)
        base_url = get_base_url(response)
        cached_url = re.sub('http://www.23us.com/', '', base_url)
        if self.cache.get(cached_url) is None:
            urls = []
            urls = base_url.split('/')
            item = ChapterItem()
            item['bookId'] = urls[len(urls) - 2]
            if self.cache.get('bk_' + item['bookId']) is not None:
                item['chapterId'] = urls[len(urls) - 1].split('.')[0]
                item['chapterName'] = sel.xpath('//h1/text()').extract()
                item['chapterContent'] = sel.xpath('//dd[@id="contents"]').extract()
            self.cache.set(cached_url, 1)
            return item

    def parse_book(self, response):
        sel = Selector(response)
        base_url = get_base_url(response)
        cached_url = re.sub('http://www.23us.com/', '', base_url)
        if self.cache.get(cached_url) is None:
            urls = []
            urls = base_url.split('/')

            item = NovelItem()
            item['bookId'] = urls[len(urls) - 1]
            if self.cache.get('bk_' + item['bookId']) is not None:
                item['book'] = sel.xpath('//div[@class="bdsub"]//dd//h1/text()').extract()[0].split(' ')[0]
                item['img'] = sel.xpath('//a[@class="hst"]/img//@src').extract()[0].strip()
                item['category'] = sel.xpath('//div[@class="bdsub"]//table[@id="at"]//td//a/text()').extract()[0].strip()
                item['author'] = sel.xpath('//div[@class="bdsub"]//table[@id="at"]//td/text()').extract()[1].strip()
                item['status'] = sel.xpath('//div[@class="bdsub"]//table[@id="at"]//td/text()').extract()[2].strip()
                item['update'] = sel.xpath('//div[@class="bdsub"]//table[@id="at"]//td/text()').extract()[5].strip()
                item['desc'] = '\n'.join(sel.xpath('//div[@class="bdsub"]//p/text()').extract())
            self.cache.set(cached_url, 1)
            return item

    def _process_request(self, request):
        info('process ' + str(request))
        return request
