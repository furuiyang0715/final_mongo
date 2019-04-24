import os
import csv
import configparser
import subprocess
# import logging

from sqlalchemy import create_engine
import pandas as pd

from sync_tables import tables

# logger = logging.getLogger(__name__)


# def mysqlcsv_cmd(conf, db, table, start, end):
#     """
#     导出 mysql 数据库 生成一个 txt 临时文件
#     :param conf:  mysql 的配置文件
#     :param db: 当前要导出的表所在数据库
#     :param table: 当前要导出的表
#     :param start: 导入的起始id
#     :param end: 导入的结束id
#     :return:
#     """
#     showposcommand = f"""mysql -h {conf['host']} -u{conf['user']} -p{conf['password']} \
#      -e 'select * from {db}.{table} where id > {start} and id <= {end};'"""
#     # logger.info(showposcommand)
#
#     txt_file = conf["txt_dir"] + f"{db}_{table}_{start}-{end}.txt"
#     with open(txt_file, "w") as f:
#         try:
#             p1 = subprocess.Popen(showposcommand, shell=True, stdout=f)
#         except Exception as e:
#             raise SystemError(e)
#     p1.wait()
#     return txt_file


# def txt2csv(file, conf):
#     w_file = open(conf['csv_dir'] + os.path.basename(file).split(".")[0]+".csv", "w")
#     # print(w_file)
#     writer_file = csv.writer(w_file)
#
#     try:
#         with open(file) as f:
#             f_csv = csv.reader(f)
#             headers = next(f_csv)
#             # 处理表头
#             writer_file.writerow(headers[0].split())
#             # 处理 row
#             for row in f_csv:
#                 writer_file.writerow(row[0].split("\t"))
#     except Exception as e:
#         raise SystemError(e)
#     finally:
#         pass
#         # 关闭写入流
#         #  writer_file.close()
#     return w_file.name

def gen_csv(conf, db, table, start, end):
    """
    使用 pandas 读取 csv 文件
    :param conf:
    :param db:
    :param table:
    :param start:
    :param end:
    :return:
    """
    mysql_string = f"mysql+pymysql://{conf['user']}:{conf['password']}@{conf['host']}:\
    {conf.getint('port')}/{conf['databases']}?charset=gbk"

    DATACENTER = create_engine(mysql_string)

    query = f"select * from {db}.{table} where id > {start} and id <= {end};"
    data = pd.read_sql(query, DATACENTER)

    file = conf["csv_dir"] + f"{db}_{table}_{start}-{end}.csv"

    data.to_csv(file, index=0)

    return file


def import2mongo(file, db, table, conf):
    """

    :param file:  即将导入的 csv 文件
    :param db:  数据库
    :param table: 表
    :param conf: mongodb的配置项
    :return:
    """
    # mongoimport --host=127.0.0.1 --db datacenter --collection table1 --type csv
    # --headerline --ignoreBlanks --file table1.csv
    showposcommand = f"mongoimport --host={conf['host']} --db {db} --collection {table} \
    --type csv --headerline --ignoreBlanks --file {file}"
    # logger.info(showposcommand)

    # drop collection if exist...
    # mongo = MyMongoDB(conf)
    # try:
    #     mongo.drop_coll(conf['databases'], table)
    # except Exception:
    #     pass

    try:
        p1 = subprocess.Popen(showposcommand, shell=True)
    except Exception as e:
        raise
        # print(f"fail to import to mongodb, because {e}")
        # raise SystemError(e)
        # return None
    p1.wait()
    return True


def load_data(conf, m_conf, db, table, start, end):
    """

    :param conf: mysql 的配置文件
    :param m_conf: mongodb的配置文件 的配置文件
    :param db: 要进行 load 的数据库
    :param table: 要进行 load 的表格
    :param start: 起始load位置
    :param end: 终止load位置

    :return:
    """
    # txt = mysqlcsv_cmd(conf, db, table, start, end)
    # logger.info(f"txt: {txt}")
    # csv = txt2csv(txt, conf)
    # logger.info(f'csv: {csv}')
    csv = gen_csv(conf, db, table, start, end)
    import2mongo(csv, db, table, m_conf)


if __name__ == "__main__":
    """测试 """
    config = configparser.ConfigParser()
    config.read('conf/config.ini')
#     _conf = config["mysql"]
#     _db = _conf.get("databases")
#     _table = tables[3]
#     _myfile = mysqlcsv_cmd(_conf, _db, _table, 200, 1000)
#     # _myfile = mysqlcsv_cmd(_conf, _db, _table, 100, 200)
#     # print(_myfile)
#     _csv_file = txt2csv(_myfile, _conf)
#     print(_csv_file)
#
#     m_conf = config['mongodb']
#     import2mongo(_csv_file, _db, _table, m_conf)

#     conf = config['mysql']
#     m_conf = config['mongodb']
#     _db = conf['databases']
#     _table = tables[13]
#     print(_table)
#     load_data(conf, m_conf, _db, _table, 0, 300)
    pass
