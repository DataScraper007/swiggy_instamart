import json
from typing import Iterable

import scrapy
from scrapy import Request
from scrapy.cmdline import execute
import swiggy_instamart.headers
from swiggy_instamart.items import ProductItem


class ScrapyProductDataSpider(scrapy.Spider):
    name = "scrapy_product_data"

    def start_requests(self):
        yield scrapy.Request(
            # url="https://www.swiggy.com/instamart/item/61WDNU5G4B",
            url="https://www.swiggy.com/instamart/item/UT3S5E9E50?storeId=1313712",
            headers=swiggy_instamart.headers.bangalore_headers,
            cookies=swiggy_instamart.headers.bangalore_cookies,
            callback=self.parse
        )

    def parse(self, response, **kwargs):
        item = ProductItem()

        # 1st approach

        # raw_data = response.xpath('//script[@type="application/ld+json"]/text()').get()

        # product_data = json.loads(raw_data)

        # item['product_name'] = product_data['name']
        # item['product_url'] = response.url
        # item['price'] = product_data['offers']['price']
        # item['mrp'] = ""
        # item['discount'] = ""
        #
        # if product_data['offers']['availability'] == "https://schema.org/InStock":
        #     item['availability'] = "InStock"
        # else:
        #     item['availability'] = "OutOfStock"

        # print(item)


        # 2nd approach
        json_data = response.xpath('//script[contains(text(),"window.___INITIAL_STATE___")]/text()').get()
        product_data = self.clean_json(json_data)['instamart']['cachedProductItemData']['lastItemState']

        variations = product_data['variations']

        item['product_name'] = product_data['product_name_without_brand'].strip(' -')
        item['product_url'] = response.url
        for data in variations:
            item['price'] = data['price']['offer_price']
            item['mrp'] = data['price']['mrp']
            item['discount'] = data['price']['offer_applied']['product_description']
            if data['inventory']['in_stock'] == True:
                item['availability'] = 'Available'
            else:
                item['availability'] = 'Not Available'

            print(item)


    def clean_json(self, raw_data):
        json_data = raw_data.replace('window.___INITIAL_STATE___ = ', '')
        start_idx = json_data.find(f'var App = {{')
        result = json_data[:start_idx].strip()

        return json.loads(result.strip(';'))

if __name__ == '__main__':
    execute('scrapy crawl scrapy_product_data'.split())
