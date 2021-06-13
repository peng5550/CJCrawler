# -*- coding: UTF-8 -*-
import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from lxml import etree
import time
import re

from utils.configRead import ReadConfig
from utils.db_connection import MySqlConnection
from utils.log import HandleLog


class CompanyCrawler:

    def __init__(self, logs, cfg):
        self.cfg = cfg
        self.logs = logs
        self.sql = MySqlConnection(logs=self.logs, cfg=self.cfg)
        self.mainpage = 'http://cjrk.hbcic.net.cn/xxgs/index.aspx'
        self.now_page = 0
        self.total_page = 0


    def __create_driver(self):
        # 创建driver
        options = webdriver.FirefoxOptions()
        # options.add_argument(
        #     'Accept="text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"')
        # options.add_argument('Accept-Encoding="gzip, deflate"')
        # options.add_argument('Accept-Language="zh-CN,zh-TW;q=0.9,zh;q=0.8,en-US;q=0.7,en;q=0.6"')
        # options.add_argument('Connection="keep-alive"')
        # options.add_argument('Content-Type="application/x-www-form-urlencoded"')
        # options.add_argument('Upgrade-Insecure-Requests="1"')
        # options.add_argument(
        #     'User-Agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0"')
        # options.add_argument('-headless')
        driver = webdriver.Firefox(firefox_options=options)
        driver.set_page_load_timeout(60)
        driver.set_script_timeout(20)
        self.driver = driver

    def __quit(self):
        # 推出driver
        if self.driver:
            self.driver.quit()

    def start(self):
        # self.logs
        self.__create_driver()
        self.wait = WebDriverWait(self.driver, 60)
        self.driver.get(self.mainpage)
        # self.driver.maximize_window()
        create_date = str(datetime.datetime.now().date())
        self.search_data(create_date)
        self.goNextPage(create_date)
        # time.sleep(5)
        self.__quit()

    def search_data(self, create_date):
        search_btn = self.wait.until(EC.element_to_be_clickable((By.ID, 'btnSearch')))
        search_btn.click()
        time.sleep(5)
        self.wait.until(EC.presence_of_element_located((By.ID, 'form1')))
        self.dataProcessing(create_date)

    def goNextPage(self, create_date):
        self.now_page = self.driver.find_element_by_css_selector("li.number.active").text
        # self.total_page = self.driver.find_element_by_id("labPageCount").text
        # while int(self.now_page) <= int(self.total_page):
        while int(self.now_page) <= 1000:
            try:
                next_page_btn = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'btn-next')))
                next_page_btn.click()
                time.sleep(5)
                self.wait.until(EC.presence_of_element_located((By.ID, 'form1')))
                self.dataProcessing(create_date)
                self.now_page = self.driver.find_element_by_css_selector("li.number.active").text
                self.logs.info(f"当前页：{self.now_page}")
                # self.total_page = self.driver.find_element_by_id("labPageCount").text
            except Exception as e:
                print(e)
                continue

    def dataProcessing(self, create_date):
        htmlText = self.driver.page_source
        html = etree.HTML(htmlText)
        TABLE_COM = self.cfg.get_cfg("MySql", "TABLE_COMPANY")
        for index, labTR in enumerate(html.xpath("//table[@class='el-table__body']/tbody/tr")):
            if index > 0 and index <= 20:
                try:
                    comItem = {
                        "company_name": labTR.xpath("td[2]/div/a/span/text()")[0].strip(),
                        "company_id": re.findall(r"sxbh=(\d+)", labTR.xpath("td[2]/a/@href")[0].strip())[0],
                        "type": labTR.xpath("td[3]/text()")[0].strip(),
                        "accredited_level": ",".join([i for i in labTR.xpath("td[4]/text()") if i.strip()]).strip(),
                        "category": labTR.xpath("td[5]/text()")[0].strip(),
                        "accept_department": labTR.xpath("td[6]/text()")[0].strip(),
                        "accept_date": labTR.xpath("td[9]/text()")[0].strip(),
                        "current_deal_status": labTR.xpath("td[10]/text()")[0].strip(),
                        "detail_url": "http://cjrk.hbcic.net.cn/xxgs/" + labTR.xpath("td[2]/a/@href")[0],
                        "create_time": create_date
                    }
                    item_info = {"company_id": comItem["company_id"]}
                    if not self.sql.select_data(table_name=TABLE_COM, cond_item=item_info):
                        self.sql.insert_data(table_name=TABLE_COM, item_info=comItem)
                    else:
                        item = {"current_deal_status": comItem["current_deal_status"]}
                        self.sql.update_data(table_name=TABLE_COM, condition_item=item_info, item_info=item)
                except Exception as e:
                    print(e)
                    continue


if __name__ == '__main__':

    cfg = ReadConfig("./conf.ini")
    logs = HandleLog("【公示信息列表采集】")
    report = CompanyCrawler(logs=logs, cfg=cfg)
    report.start()
