from detailInfo import ReportCrawler
from indexInfo import CompanyCrawler
from utils.configRead import ReadConfig
from utils.log import HandleLog


if __name__ == '__main__':

    cfg = ReadConfig("./conf.ini")
    logs = HandleLog("【公示信息列表采集】")
    report = CompanyCrawler(logs=logs, cfg=cfg)
    report.start()


    logs = HandleLog("【公示信息详情采集】")
    report = ReportCrawler(logs=logs, cfg=cfg)
    report.start()