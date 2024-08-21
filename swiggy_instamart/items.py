# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ProductItem(scrapy.Item):
    product_name = scrapy.Field()
    product_url = scrapy.Field()
    availability = scrapy.Field()
    price = scrapy.Field()
    discount = scrapy.Field()
    mrp = scrapy.Field()
    pincode = scrapy.Field()

class urlItem(scrapy.Item):
    id = scrapy.Field()
    code = scrapy.Field()