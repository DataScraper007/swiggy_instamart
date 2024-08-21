# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from swiggy_instamart.connections import update_status_code
from swiggy_instamart.connections import insert_products

class SwiggyInstamartPipeline:
    def process_item(self, item, spider):
        # update_status_code(item['id'], item['code'])
        insert_products(item)