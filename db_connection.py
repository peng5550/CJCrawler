# coding: utf8
import pymysql


class MySqlConnection:

    def __init__(self, logs, cfg):
        self.sql_conn(cfg)
        self.logs = logs

    def sql_conn(self, cfg):
        host = cfg.get_cfg("MySql", "HOST")
        port = cfg.get_cfg("MySql", "PORT")
        dbName = cfg.get_cfg("MySql", "DB")
        username = cfg.get_cfg("MySql", "USERNAME")
        password = cfg.get_cfg("MySql", "PASSWORD")
        self.conn = pymysql.connect(host=host, port=int(port), db=dbName, user=username, password=password)
        self.db = self.conn.cursor()

    def select_data(self, table_name, search_keys=None, cond_item=None):
        if not search_keys:
            search_keys = ["*"]
        SELECT_SQL = "SELECT DISTINCT {} FROM {}".format(", ".join(search_keys), table_name)
        if cond_item:
            cond_str_list = []
            for key, value in cond_item.items():
                cond_str_list.append(f"{key}='{value}'")

            cond_str = " AND ".join(cond_str_list)
            SELECT_SQL += f" WHERE {cond_str}"

        self.db.execute(SELECT_SQL)
        res = self.db.fetchall()
        if res:
            return res
        else:
            return False

    def select_data_(self):
        SELECT_SQL = "select company_name, detail_url, type, category, company_id from cim_company_info where create_time = (select max(create_time) from cim_company_info)"
        self.db.execute(SELECT_SQL)
        res = self.db.fetchall()
        if res:
            return res
        else:
            return False


    def insert_data(self, item_info, table_name):
        keys = ', '.join(list(item_info.keys()))
        values = ', '.join([f"%s" for i in range(len(item_info))])
        insert_sql = "INSERT INTO {}({})VALUES({})".format(table_name, keys, values)
        try:
            self.db.execute(insert_sql, tuple(item_info.values()))
            self.conn.commit()
            self.logs.info("【数据存储成功】-{}".format(item_info))
        except Exception as e:
            self.logs.error("【数据存储失败】-{}-{}".format(e, item_info))
            self.conn.rollback()

    def update_data(self, item_info, condition_item, table_name):
        VALUE_STR = ", ".join([f"{key}='{value}'" for key, value in item_info.items()])
        string_list = []
        for i in condition_item.keys():
            string = "%s='%s'" % (i, condition_item.get(i))
            string_list.append(string)
        CON_STR = ' AND '.join(string_list)
        update_sql = "UPDATE {} SET {} WHERE {}".format(table_name, VALUE_STR, CON_STR)
        try:
            self.db.execute(update_sql)
            self.conn.commit()
            self.logs.info("【数据已存在，更新成功-{}】".format(item_info))
        except Exception as e:
            self.logs.error("【数据已存在，数据更新失败】-{}-{}".format(e, item_info))
            self.conn.rollback()

    def select_data_mqd(self, table_name):

        select_sql = "select platform , store_name ,product_link from {} where platform != '淘宝天猫' group by platform ,store_name".format(
            table_name)
        self.db.execute(select_sql)
        res = self.db.fetchall()
        if res:
            return res
        else:
            return False

