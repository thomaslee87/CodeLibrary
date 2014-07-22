# Scrapy settings for NovelCrawler project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'NovelCrawler'

SPIDER_MODULES = ['NovelCrawler.spiders']
NEWSPIDER_MODULE = 'NovelCrawler.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'NovelCrawler (+http://www.yourdomain.com)'

ITEM_PIPELINES = {
    'NovelCrawler.pipelines.JsonWithEncodingNovelPipeline': 300,
}

DOWNLOADER_MIDDLEWARES = {  
    'scrapy.contrib.downloadermiddleware.useragent.UserAgentMiddleware' : None,  
    'NovelCrawler.spiders.rotate_useragent.RotateUserAgentMiddleware' :400  
}  

#LOG_LEVEL = 'INFO'
COOKIES_ENABLES=False

SCHEDULER_ORDER='BFO'

