import os
import random
import json
import sys
import time
from datetime import datetime

import pymysql
from curl_cffi import requests
from lxml import html
from pymysql import IntegrityError

db_host = 'localhost'
db_user = 'root'
db_password = 'actowiz'
db_port = 3306
db_name = 'qcg'
delivery_date = str(datetime.today().strftime("%Y%m%d"))
db_data_table = f"swiggy_instamart_{delivery_date}"
db_log_table = f"swiggy_instamart_logs_{delivery_date}"


class SwiggyInstamartScraper:
    def __init__(self, pincode):
        self.pincode = pincode
        self.input_id = None
        self.flag = None
        self.cookies = json.loads(
            open(r'C:\Users\Admin\PycharmProjects\QC\QC\cookies\swiggy_instamart_cookies_updated.json', 'r').read())[
            str(pincode)]
        self.con = pymysql.connect(host=db_host, user=db_user, password=db_password, database=db_name)
        self.cursor = self.con.cursor()
        self.page_save_pdp = fr'C:\Users\Admin\PycharmProjects\page_save\{delivery_date}\swi\HTMLS'
        self.pincode_id = self.cursor.execute('select id from pincodes where pincode=%s', pincode)
        create_table = f"""CREATE TABLE IF NOT EXISTS `{db_data_table}` (`Id` INT NOT NULL AUTO_INCREMENT,
                                                                                `comp` VARCHAR (255) DEFAULT 'N/A',
                                                                                `fk_id` VARCHAR (255) DEFAULT 'N/A',
                                                                                `pincode` VARCHAR (255) DEFAULT 'N/A',
                                                                                `url` VARCHAR (255) DEFAULT 'N/A',
                                                                                `name` VARCHAR (255) DEFAULT 'N/A',
                                                                                `availability` VARCHAR (255) DEFAULT 'N/A',
                                                                                `price` VARCHAR (255) DEFAULT 'N/A',
                                                                                `discount` VARCHAR (255) DEFAULT 'N/A',
                                                                                `mrp` VARCHAR (255) DEFAULT 'N/A',
                                                                                PRIMARY KEY (`Id`),
                                                                                UNIQUE KEY `fid` (`fk_id`,`pincode`)
                                                                                ) ENGINE = InnoDB DEFAULT CHARSET = UTF8MB4;
                        """
        self.cursor.execute(create_table)
        create_log_table = f"""CREATE TABLE IF NOT EXISTS `{db_log_table}` (`id` INT NOT NULL AUTO_INCREMENT,
                                                                                pincode_id INT NOT NULL,
                                                                                input_swi_id INT NOT NULL,
                                                                                status VARCHAR (255) NOT NULL,
                                                                                PRIMARY KEY (`id`),
                                                                                UNIQUE KEY `log_id` (`input_swi_id`,`pincode_id`)
                                                                                ) ENGINE = InnoDB DEFAULT CHARSET = UTF8MB4;
                        """
        self.cursor.execute(create_log_table)

    def start_requests(self):
        if not self.cookies:
            return
        self.cursor.execute(
            f'''SELECT fkg_pid, swi_url, Id FROM input_swi
            WHERE swi_url != "NA" AND fkg_pid IN
            (SELECT pid FROM pid_pincode WHERE pincode="{self.pincode}")'''
        )
        results = self.cursor.fetchall()

        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'max-age=0',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        }
        # self.cursor.execute(f'select input_swi_id, pincode_id, status from {db_log_table} where status != "ERROR"')
        # log_data = self.cursor.fetchall()  # Fetch all results
        #

        # data_dict = {row[0]: str(row[1]) + "_" + row[2] for row in log_data}

        for result in results:
            fkg_pid = result[0]
            swi_url = result[1]
            self.input_id = result[2]
            self.cursor.execute(f'select input_swi_id, pincode_id, status from {db_log_table} where input_swi_id = %s and pincode_id = %s', (self.input_id, self.pincode_id))
            log_data = self.cursor.fetchone() # Fetch all results
            if log_data:
                print(log_data)
                if log_data[2] == "ERROR":
                    self.flag = "UPDATE"
                else:
                    continue
            self.fetch_page(fkg_pid, swi_url, headers)
            time.sleep(random.choice([0.1, 0.2, 0.3, 0.4]))

    def fetch_page(self, fkg_pid, swi_url, headers):
        try:
            browsers = [
                "edge99",
                "chrome110",
                "chrome101",
                "safari15_5"
            ]
            browser = random.choice(browsers)
            proxies = {
                "http": "http://scraperapi:3e82b5ff05418c9376d9823c799c1b14@proxy-server.scraperapi.com:8001"
            }
            response = requests.get(swi_url,
                                    headers=headers, cookies=self.cookies, impersonate=browser, proxies=proxies)
            response.raise_for_status()

            if not os.path.exists(self.page_save_pdp):
                os.makedirs(self.page_save_pdp)

            with open(f"{self.page_save_pdp}{fkg_pid}{self.pincode}.html", "w", encoding="utf-8") as file:
                file.write(response.text)

            self.parse(response, fkg_pid)

        except Exception as e:
            print("ERROR: ", e)
            if self.flag == "UPDATE":
                self.cursor.execute(f"UPDATE {db_log_table} set status='ERROR' where input_swi_id=%s and pincode_id=%s",
                                    (self.input_id, self.pincode_id))
                self.con.commit()
            else:
                self.cursor.execute(f"INSERT INTO {db_log_table} (input_swi_id, pincode_id, status) values (%s,%s,%s)",
                                    (self.input_id, self.pincode_id, 'ERROR'))
                self.con.commit()

    def parse(self, response, fkg_pid):
        item = {}
        json_data = html.fromstring(response.content).xpath(
            '//script[contains(text(),"window.___INITIAL_STATE___")]/text()')

        if json_data:
            product_data = self.clean_json(json_data[0])['instamart']['cachedProductItemData']

            item['comp'] = 'Swiggy Instamart'
            item['url'] = response.url
            item['fk_id'] = fkg_pid
            item['pincode'] = self.pincode

            if product_data:
                product_data = product_data['lastItemState']
                variations = product_data['variations']
                data = variations[0]
                item['name'] = data['display_name'] + ' ' + data['sku_quantity_with_combo']
                item['price'] = data['price']['offer_price']
                item['mrp'] = data['price']['mrp']
                item['discount'] = data['price']['offer_applied']['product_description']
                item['availability'] = data['inventory']['in_stock']
                if self.flag == "UPDATE":
                    self.cursor.execute(
                        f"UPDATE {db_log_table} set status='SUCCESS' where input_swi_id=%s and pincode_id=%s",
                        (self.input_id, self.pincode_id))
                    self.con.commit()
                else:
                    self.cursor.execute(
                        f"INSERT INTO {db_log_table} (input_swi_id, pincode_id, status) values (%s,%s,%s)",
                        (self.input_id, self.pincode_id, 'SUCCESS'))
                    self.con.commit()
                self.db_store(item)
            else:
                self.handle_error(response, fkg_pid, item)
        else:
            self.handle_error(response, fkg_pid, item)

    def handle_error(self, response, fkg_pid, item):
        error_msg = html.fromstring(response.content).xpath(
            '//div[contains(text(),"Our best minds are on it. You may retry or check back soon")]/text()'
        )

        if error_msg:
            if self.flag == "UPDATE":
                self.cursor.execute(f"UPDATE {db_log_table} set status='ERROR' where input_swi_id=%s and pincode_id=%s",
                                    (self.input_id, self.pincode_id))
                self.con.commit()
            else:
                self.cursor.execute(f"INSERT INTO {db_log_table} (input_swi_id, pincode_id, status) values (%s,%s,%s)",
                                    (self.input_id, self.pincode_id, 'ERROR'))
                self.con.commit()
        else:
            item['name'] = ""
            item['price'] = ""
            item['mrp'] = ""
            item['discount'] = ""
            item['availability'] = False
            self.db_store(item)
            if self.flag == "UPDATE":
                self.cursor.execute(
                    f"UPDATE {db_log_table} set status='SUCCESS' where input_swi_id=%s and pincode_id=%s",
                    (self.input_id, self.pincode_id))
                self.con.commit()
            else:
                self.cursor.execute(f"INSERT INTO {db_log_table} (input_swi_id, pincode_id, status) values (%s,%s,%s)",
                                    (self.input_id, self.pincode_id, 'SUCCESS'))
                self.con.commit()

    def clean_json(self, raw_data):
        json_data = raw_data.replace('window.___INITIAL_STATE___ = ', '')
        start_idx = json_data.find('var App = {')
        result = json_data[:start_idx].strip()

        return json.loads(result.strip(';'))

    def db_store(self, item):
        try:
            field_list = []
            value_list = []

            for field in item:
                field_list.append(str(field))
                value_list.append('%s')
            fields = ','.join(field_list)
            values = ", ".join(value_list)
            insert_db = f"insert into {db_data_table}( " + fields + " ) values ( " + values + " )"
            try:
                self.cursor.execute(insert_db, tuple(item.values()))
                self.con.commit()
                print('Data Inserted...')
            except IntegrityError as e:
                print(e)
        except Exception as e:
            print(e)


if __name__ == '__main__':
    scraper = SwiggyInstamartScraper(pincode=sys.argv[1])
    scraper.start_requests()
