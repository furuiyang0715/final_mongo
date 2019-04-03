import datetime
import decimal
import pymysql
import logging.config


class SysException(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class MySql:
    def __init__(self, conf, logger):
        """
        :param conf: 针对 mysql 的配置
        :param logger:
        """
        self.logger = logger
        self.conf = conf

    def gen_con(self):
        try:
            con = pymysql.connect(
                host=self.conf['host'],
                port=self.conf.getint('port'),
                user=self.conf['user'],
                password=self.conf['password'],
                charset='utf8mb4',
                db=self.conf['databases']
            )
        except Exception:
            raise
        return con

    def get_now_id(self, tables):
        """
        获取当前mysql的最大id信息
        :param tables:
        :return:
        """
        if not isinstance(tables, list):
            tables = [tables]

        query_sql = """select max(id) from {};"""
        # print(query_sql)

        res_dict = dict()

        # 创建一个mysql连接
        con = self.gen_con()

        try:
            with con.cursor() as cursor:
                for table_name in tables:
                    q_sql = query_sql.format(table_name)
                    # print("-----> ", q_sql)
                    cursor.execute(q_sql)
                    res = cursor.fetchall()

                    try:
                        table_length = res[0][0]
                    except Exception:
                        raise

                    res_dict.update({table_name: table_length})
        except Exception:
            raise SystemError
        finally:
            con.commit()
        return res_dict

    def zip_doc_dict(self, name_list, column_tuple):
        if len(name_list) != len(column_tuple):
            return None

        name_tuple = tuple(name_list)
        column_dict = dict(zip(name_tuple, column_tuple))
        return column_dict

    def td_format(self, td_object):
        seconds = int(td_object.total_seconds())
        return seconds

    def check_each_sql_table_data(self, dict_data):
        #  在测试的过程中发现需要转换的类型有：
        #  (1) decimal.Decimal
        # （2) datetime.timedelta(seconds=75600)

        for key, value in dict_data.items():
            if isinstance(value, decimal.Decimal):
                if value.as_tuple().exponent == 0:
                    dict_data[key] = int(value)
                else:
                    dict_data[key] = float(value)

            elif isinstance(value, datetime.timedelta):
                dict_data[key] = self.td_format(value)

        return dict_data

    def gen_sql_table_name_list(self, connection):
        query_sql ="""select table_name from information_schema.tables where 
        table_schema="{}";""".format(self.mysql_DBname)
        sql_table_name_list = list()
        try:
            with connection.cursor() as cursor:
                cursor.execute(query_sql)
                res = cursor.fetchall()
                for column in res:
                    sql_table_name_list.append(column[0])

        except Exception:
            raise
        finally:
            connection.commit()
        return sql_table_name_list

    def gen_sql_table_datas_list(self, connection, table_name, name_list, pos):
        try:
            with connection.cursor() as cursor:
                # num 的值在同步的时候可以设置较大 且不打印数据 在增量更新阶段 可以设置小一点 且在日志中打印插入的 items
                num = 10
                start = pos
                while True:
                    query_sql = """
                    select * from {} limit {},{};""".format(table_name, start, num)
                    print(query_sql)
                    sys.exit(0)

                    cursor.execute(query_sql)

                    res = cursor.fetchall()
                    if not res:
                        break
                    start += num

                    yield_column_list = list()
                    for column in res:
                        column_dict = self.zip_doc_dict(name_list, column)
                        yield_column_list.append(column_dict)
                    yield yield_column_list
        except Exception as e:
            print(f'gen table data list 失败， {table_name} at position {pos}, 原因 {e}')
            # raise SystemError(e)
        finally:
            connection.commit()
