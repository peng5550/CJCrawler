# -*- coding: UTF-8 -*-
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from lxml import etree, html
import re
import time

from utils.configRead import ReadConfig
from utils.db_connection import MySqlConnection
from utils.log import HandleLog


class ReportCrawler(object):

    def __init__(self, logs, cfg):
        self.logs = logs
        self.cfg = cfg
        self.sql = MySqlConnection(logs=self.logs, cfg=self.cfg)
        self.now_index = 0
        self.totals = 0
        self.company_table = self.cfg.get_cfg("MySql", "TABLE_COMPANY")
        self.person_table = self.cfg.get_cfg("MySql", "TABLE_PERSON")
        self.html_table = self.cfg.get_cfg("MySql", "TABLE_HTML")
        self.company_name_new_table = self.cfg.get_cfg("MySql", "TABLE_NEW_NAME")

    def __create_driver(self):
        # 创建driver
        try:
            options = webdriver.FirefoxOptions()
            # options.add_argument(
            #     'Accept="text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"')
            # options.add_argument('Accept-Encoding="gzip, deflate"')
            # options.add_argument('Accept-Language="zh-CN,zh-TW;q=0.9,zh;q=0.8,en-US;q=0.7,en;q=0.6"')
            # options.add_argument('Connection="keep-alive"')
            # options.add_argument(
            #     'User-Agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0"')
            # options.add_argument('-headless')
            driver = webdriver.Firefox(executable_path=f"geckodriver.exe", firefox_options=options)
            driver.set_page_load_timeout(60)
            driver.set_script_timeout(20)
            return driver
        except Exception as e:
            self.logs.info(f"【创建driver失败,{e}】")

    def __quit(self, driver):
        if driver:
            driver.quit()

    def getUrlFromSql(self):
        # urlList = self.sql.select_data(table_name=self.company_table, search_keys=["company_name", "detail_url", "type",
        #                                                                            "category", "company_id"])
        totalData = self.sql.select_data_()
        return [[i[0], i[1], i[2], i[3], i[4]] for i in totalData if i]

    def saveItem(self, infoReportList, html0, companyId):

        tableHtml = html0.xpath("//*[@id='form1']/table/tbody/tr[1]/td[1]/table/tbody/tr[4]")[0]
        [elem.getparent().remove(elem) for elem in tableHtml.xpath("//div[@class='dibu']")]
        tableHtmlText = re.sub(r'(<img.*>?)|(<span id="labNowPlace".*?>)', '',
                               html.tostring(tableHtml, encoding="utf-8").decode("utf-8"))

        html_check = {"company_id": companyId}
        if not self.sql.select_data(table_name=self.html_table, cond_item=html_check):
            item = {"company_id": companyId, "table_text": tableHtmlText}
            self.sql.insert_data(table_name=self.html_table, item_info=item)

        for item in infoReportList:
            if item["name"] != "姓名":
                cond_item = {"company_id": item["company_id"], "name": item["name"],
                             "company_name": item["company_name"]}
                if not self.sql.select_data(table_name=self.person_table, cond_item=cond_item):
                    self.sql.insert_data(table_name=self.person_table, item_info=item)

    def getHtml(self, driver, wait, link):
        try:
            driver.get(link)
            wait.until(EC.presence_of_element_located((By.ID, "form1")))
            htmlText = driver.page_source
            return htmlText
        except:
            pass

    def save_new_company_name(self, htmlText, company_name, company_id):
        company_name_new = re.findall(r"新企业：([\s\S]*?)<", htmlText, re.M)[0].strip()
        item = {"company_id": company_id, "company_name": company_name, "company_name_new": company_name_new}
        if not self.sql.select_data(table_name=self.company_name_new_table, cond_item=item):
            self.sql.insert_data(item_info=item, table_name=self.company_name_new_table)

    def dataProcessing4JZ(self, htmlText, companyName, companyId, category):
        if re.findall(r"分立|合并|吸收|新企业：", category):
            self.save_new_company_name(htmlText, companyName, companyId)
        try:
            html0 = etree.HTML(htmlText)
            labTDListA = [labTD.xpath("text()")[0].strip() for labTD in
                          html0.xpath("//*[@id='fs2']/table[@class='table']/tbody/tr[1]/td")]
            labTDListB = [labTD.xpath("text()")[0].strip() for labTD in
                          html0.xpath("//*[@id='fs3']/table[@class='table']/tbody/tr[1]/td")]

            if "内容" in labTDListA:
                companyIndexA = labTDListA.index("内容")
            else:
                companyIndexA = None

            if "内容" in labTDListB:
                companyIndexB = labTDListB.index("内容")
            else:
                companyIndexB = None

            nameIndexA = labTDListA.index("姓名")
            ageIndexA = labTDListA.index("年龄")
            majorIndexA = labTDListA.index("职称专业")
            posiTitIndexA = labTDListA.index("职称")

            nameIndexB = labTDListB.index("姓名")
            ageIndexB = labTDListB.index("年龄")
            majorIndexB = labTDListB.index("专业")
            posiTitIndexB = labTDListB.index("证书编号")

            labTRListA = html0.xpath("//*[@id='fs2']/table[@class='table']/tbody/tr")[1:]
            labTRListB = html0.xpath("//*[@id='fs3']/table[@class='table']/tbody/tr")[1:]
            infoReportList = []
            for labTR in labTRListA:
                if companyIndexA:
                    companyName = labTR.xpath("td")[companyIndexA].xpath("text()")[0].strip().replace("原：", "").replace("新：", "")
                comItem = {
                    "name": labTR.xpath("td")[nameIndexA].xpath("text()")[0].strip(),
                    "age": labTR.xpath("td")[ageIndexA].xpath("text()")[0].strip(),
                    "title_major": labTR.xpath("td")[majorIndexA].xpath("text()")[0].strip(),
                    "title": labTR.xpath("td")[posiTitIndexA].xpath("text()")[0].strip(),
                    "company_name": companyName,
                    "company_id": companyId
                }
                infoReportList.append(comItem)

            for labTR in labTRListB:
                if companyIndexB:
                    companyName = labTR.xpath("td")[companyIndexB].xpath("text()")[0].strip().replace("原：", "").replace("新：", "")
                comItem = {
                    "name": labTR.xpath("td")[nameIndexB].xpath("text()")[0].strip(),
                    "age": labTR.xpath("td")[ageIndexB].xpath("text()")[0].strip(),
                    "title_major": labTR.xpath("td")[majorIndexB].xpath("text()")[0].strip(),
                    "title": re.sub(r'(\\)|(")', "", labTR.xpath("td")[posiTitIndexB].xpath("text()")[0].strip()),
                    "company_name": companyName,
                    "company_id": companyId
                }
                infoReportList.append(comItem)

            return infoReportList, html0, companyId

        except Exception as e:
            self.logs.info(f"【dataProcessing4JZHTML解析失败, {e}】")
            return None, None, None

    def dataProcessing4AXZ(self, htmlText, companyName, companyId, category):
        if re.findall(r"分立|合并|吸收|新企业：", category):
            self.save_new_company_name(htmlText, companyName, companyId)
        try:
            html0 = etree.HTML(htmlText)
            infoReportList = []
            labTDNameList = html0.xpath("//*[@id='fs1']/table//td[contains(text(), '姓名')]/text()")
            labTDZCList = html0.xpath("//*[@id='fs1']/table//td[contains(text(), '职务')]/text()")
            labTDZYList = html0.xpath("//*[@id='fs1']/table//td[contains(text(), '证书编号')]/text()")
            for name, title_major, title in zip(labTDNameList, labTDZCList, labTDZYList):
                if "姓名" not in name:
                    comItem = {
                        "name": name.strip(),
                        "age": 0,
                        "title_major": title_major.strip(),
                        "title": title.strip(),
                        "company_name": companyName,
                        "company_id": companyId
                    }
                    infoReportList.append(comItem)


            labTDListA = [labTD.xpath("text()")[0].strip() for labTD in
                          html0.xpath("//*[@id='fs2']/div[1]/table[@class='table']/tbody/tr[1]/td")]
            nameIndexA = labTDListA.index("姓名")
            majorIndexA = labTDListA.index("专业")
            posiTitIndexA = labTDListA.index("证书编号")
            labTRListA = html0.xpath("//*[@id='fs2']/div/table[@class='table']/tbody/tr")[1:]
            for labTR in labTRListA:
                if "姓名" not in labTR.xpath("td")[nameIndexA].xpath("text()")[0].strip():
                    comItem = {
                        "name": labTR.xpath("td")[nameIndexA].xpath("text()")[0].strip(),
                        "age": 0,
                        "title_major": labTR.xpath("td")[majorIndexA].xpath("text()")[0].strip(),
                        "title": labTR.xpath("td")[posiTitIndexA].xpath("text()")[0].strip(),
                        "company_name": companyName,
                        "company_id": companyId
                    }
                    infoReportList.append(comItem)

            return infoReportList, html0, companyId
        except Exception as e:
            self.logs.info(f"【dataProcessing4AXZ解析失败, {e}】")
            return None, None, None

    def dataProcessing4GCKC(self, htmlText, companyName, companyId, category):
        if re.findall(r"分立|合并|吸收|新企业：", category):
            self.save_new_company_name(htmlText, companyName, companyId)

        try:
            html0 = etree.HTML(htmlText)
            infoReportList = []

            labTDListA = [labTD.xpath("text()")[0].strip() for labTD in
                          html0.xpath("//*[@id='fs2']/div[2]/table[@class='table']/tbody/tr[1]/td")]
            nameIndexA = labTDListA.index("姓名")
            ageIndexA = labTDListA.index("年龄")
            majorIndexA = labTDListA.index("从事专业")
            labTRListA = html0.xpath("//*[@id='fs2']/div[2]/table[@class='table']/tbody/tr")[1:]
            for labTR in labTRListA:
                if "姓名" not in labTR.xpath("td")[nameIndexA].xpath("text()")[0].strip():
                    comItem = {
                        "name": labTR.xpath("td")[nameIndexA].xpath("text()")[0].strip(),
                        "age": labTR.xpath("td")[ageIndexA].xpath("text()")[0].strip(),
                        "title_major": labTR.xpath("td")[majorIndexA].xpath("text()")[0].strip(),
                        "title": "",
                        "company_name": companyName,
                        "company_id": companyId
                    }
                    infoReportList.append(comItem)

            return infoReportList, html0, companyId
        except Exception as e:
            self.logs.info(f"【dataProcessing4GCKC解析失败, {e}】")
            return None, None, None

    def dataProcessing4ZJZX(self, htmlText, companyName, companyId, category):
        if re.findall(r"分立|合并|吸收|新企业：", category):
            self.save_new_company_name(htmlText, companyName, companyId)
        try:
            html0 = etree.HTML(htmlText)
            infoReportList = []

            labTDListA = [labTD.xpath("text()")[0].strip() for labTD in
                          html0.xpath("//*[@id='fs3']/table[@class='table']/tbody/tr[1]/td")]
            nameIndexA = labTDListA.index("姓名")
            ageIndexA = labTDListA.index("年龄")
            majorIndexA = labTDListA.index("职称")
            posiTitIndexA = labTDListA.index("造价工程师注册证书编号/造价员证书编号")
            labTRListA = html0.xpath("//*[@id='fs3']/table[@class='table']/tbody/tr")[1:]
            for labTR in labTRListA:
                if "姓名" not in labTR.xpath("td")[nameIndexA].xpath("text()")[0].strip():
                    comItem = {
                        "name": labTR.xpath("td")[nameIndexA].xpath("text()")[0].strip(),
                        "age": labTR.xpath("td")[ageIndexA].xpath("text()")[0].strip(),
                        "title_major": labTR.xpath("td")[majorIndexA].xpath("text()")[0].strip(),
                        "title": labTR.xpath("td")[posiTitIndexA].xpath("text()")[0].strip(),
                        "company_name": companyName,
                        "company_id": companyId
                    }
                    infoReportList.append(comItem)

            return infoReportList, html0, companyId
        except Exception as e:
            self.logs.info(f"【dataProcessing4ZJZX解析失败, {e}】")
            return None, None, None

    def dataProcessing4FDCKF(self, htmlText, companyName, companyId, category):
        if re.findall(r"分立|合并|吸收|新企业：", category):
            self.save_new_company_name(htmlText, companyName, companyId)
        try:
            html0 = etree.HTML(htmlText)
            infoReportList = []

            labTDListA = [labTD.xpath("text()")[0].strip() for labTD in
                          html0.xpath("//*[@id='fs2']/table[@class='table']/tbody/tr[1]/td")]
            nameIndexA = labTDListA.index("姓名")
            majorIndexA = labTDListA.index("职称专业")
            labTRListA = html0.xpath("//*[@id='fs2']/table[@class='table']/tbody/tr")[1:]
            for labTR in labTRListA:
                if "姓名" not in labTR.xpath("td")[nameIndexA].xpath("text()")[0].strip():
                    comItem = {
                        "name": labTR.xpath("td")[nameIndexA].xpath("text()")[0].strip(),
                        "age": 0,
                        "title_major": labTR.xpath("td")[majorIndexA].xpath("text()")[0].strip(),
                        "company_name": companyName,
                        "company_id": companyId
                    }
                    infoReportList.append(comItem)

            return infoReportList, html0, companyId
        except Exception as e:
            self.logs.info(f"【dataProcessing4FDCKF解析失败, {e}】")
            return None, None, None

    def dataProcessing4GCJL(self, htmlText, companyName, companyId, category):
        if re.findall(r"分立|合并|吸收|新企业：", category):
            self.save_new_company_name(htmlText, companyName, companyId)
        try:
            html0 = etree.HTML(htmlText)
            infoReportList = []

            labTDListA = [labTD.xpath("text()")[0].strip() for labTD in
                          html0.xpath("//*[@id='fs2']/table[@class='table']/tbody/tr[1]/td")]
            nameIndexA = labTDListA.index("姓名")
            ageIndexA = labTDListA.index("年龄")
            majorIndexA = labTDListA.index("类型")
            posiTitIndexA = labTDListA.index("注册证书编号")
            labTRListA = html0.xpath("//*[@id='fs2']/table[@class='table']/tbody/tr")[1:]
            for labTR in labTRListA:
                if "姓名" not in labTR.xpath("td")[nameIndexA].xpath("text()")[0].strip():
                    comItem = {
                        "name": labTR.xpath("td")[nameIndexA].xpath("text()")[0].strip(),
                        "age": labTR.xpath("td")[ageIndexA].xpath("text()")[0].strip(),
                        "title": labTR.xpath("td")[posiTitIndexA].xpath("text()")[0].strip(),
                        "title_major": labTR.xpath("td")[majorIndexA].xpath("text()")[0].strip(),
                        "company_name": companyName,
                        "company_id": companyId
                    }
                    infoReportList.append(comItem)

            return infoReportList, html0, companyId
        except Exception as e:
            self.logs.info(f"【dataProcessing4GCJL解析失败, {e}】")
            return None, None, None

    def dataProcessing4GLJC(self, htmlText, companyName, companyId, category):
        if re.findall(r"分立|合并|吸收|新企业：", category):
            self.save_new_company_name(htmlText, companyName, companyId)

        try:
            html0 = etree.HTML(htmlText)
            infoReportList = []

            labTDListA = [labTD.xpath("text()")[0].strip() for labTD in
                          html0.xpath("//*[@id='fs2']/table[@class='table']/tbody/tr[1]/td")]
            nameIndexA = labTDListA.index("姓名")
            ageIndexA = labTDListA.index("年龄")
            labTRListA = html0.xpath("//*[@id='fs2']/table[@class='table']/tbody/tr")[1:]
            for labTR in labTRListA:
                if "姓名" not in labTR.xpath("td")[nameIndexA].xpath("text()")[0].strip():
                    comItem = {
                        "name": labTR.xpath("td")[nameIndexA].xpath("text()")[0].strip(),
                        "age": labTR.xpath("td")[ageIndexA].xpath("text()")[0].strip(),
                        "title": "",
                        "title_major": "",
                        "company_name": companyName,
                        "company_id": companyId
                    }
                    infoReportList.append(comItem)

            labTDListB = [labTD.xpath("text()")[0].strip() for labTD in
                          html0.xpath("//*[@id='fs3']/table[@class='table']/tbody/tr[1]/td")]
            nameIndexB = labTDListB.index("姓名")
            ageIndexB = labTDListB.index("年龄")
            majorIndexB = labTDListB.index("注册专业")
            posiTitIndexB = labTDListB.index("注册证书编号")
            labTRListB = html0.xpath("//*[@id='fs3']/table[@class='table']/tbody/tr")[1:]
            for labTR in labTRListB:
                if "姓名" not in labTR.xpath("td")[nameIndexA].xpath("text()")[0].strip():
                    comItem = {
                        "name": labTR.xpath("td")[nameIndexB].xpath("text()")[0].strip(),
                        "age": labTR.xpath("td")[ageIndexB].xpath("text()")[0].strip(),
                        "title_major": labTR.xpath("td")[majorIndexB].xpath("text()")[0].strip(),
                        "title": labTR.xpath("td")[posiTitIndexB].xpath("text()")[0].strip(),
                        "company_name": companyName,
                        "company_id": companyId
                    }
                    infoReportList.append(comItem)

            return infoReportList, html0, companyId
        except Exception as e:
            self.logs.info(f"【dataProcessing4GLJC解析失败, {e}】")
            return None, None, None

    def __crawler(self, urlList):
        driver = self.__create_driver()
        wait = WebDriverWait(driver, 60)
        self.totals = len(urlList)
        for name, link, type_, category, company_id in urlList:
            try:
                _content = self.getHtml(driver, wait, link)
                if type_ == "建筑业":
                    infoReportList, html0, companyId = self.dataProcessing4JZ(_content, name, company_id, category)
                elif type_ == "安许证":
                    infoReportList, html0, companyId = self.dataProcessing4AXZ(_content, name, company_id, category)
                elif type_ == '工程勘察' or type_ == '工程设计':
                    infoReportList, html0, companyId = self.dataProcessing4GCKC(_content, name, company_id, category)
                elif type_ == '工程监理':
                    infoReportList, html0, companyId = self.dataProcessing4GCJL(_content, name, company_id, category)
                elif type_ == '造价咨询':
                    infoReportList, html0, companyId = self.dataProcessing4ZJZX(_content, name, company_id, category)
                elif type_ == '质量检测':
                    infoReportList, html0, companyId = self.dataProcessing4GLJC(_content, name, company_id, category)
                elif type_ == '房地产开发':
                    infoReportList, html0, companyId = self.dataProcessing4FDCKF(_content, name, company_id, category)

                else:
                    infoReportList = html0 = companyId = None

                if infoReportList:
                    self.saveItem(infoReportList, html0, companyId)
            except Exception as e:
                self.logs.info(f"【__crawler error {e}】")
                with open("exception.txt", "a+", encoding="utf-8")as file:
                    file.write(link + "\n")
            self.now_index += 1
            self.logs.info(f"【当前第{self.now_index}条/{self.totals}】")
            time.sleep(1)
        self.__quit(driver)

    def start(self):
        self.logs.info("【公示信息详情采集】")
        urlList = self.getUrlFromSql()
        self.logs.info(f"【共获取到{len(urlList)}条任务】")
        self.logs.info("【任务开始】")
        self.__crawler(urlList)
        self.logs.info("【任务结束】")


if __name__ == '__main__':
    cfg = ReadConfig("./conf.ini")
    logs = HandleLog("【公示信息详情采集】")
    report = ReportCrawler(logs=logs, cfg=cfg)
    report.start()
