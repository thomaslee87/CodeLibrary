# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field

class NovelItem(Item):
    # define the fields for your item here like:
    # name = Field()
    book = Field()
    bookId = Field()
    img  = Field()
    category = Field()
    author = Field()
    status = Field()
    update = Field()
    desc = Field()

class ChapterItem(Item):
    bookId = Field()
    chapterId = Field()
    chapterName = Field()
    chapterContent = Field()
