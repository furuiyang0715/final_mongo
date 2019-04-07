import configparser
import csv
import subprocess

from sqlalchemy import create_engine
import pandas as pd


config = configparser.ConfigParser()
config.read('conf/config.ini')

conf = config['mysql']

mysql_string = f"mysql+pymysql://{conf['user']}:{conf['password']}@{conf['host']}:\
{conf.getint('port')}/{conf['databases']}?charset=gbk"

DATACENTER = create_engine(mysql_string)

query = "select * from datacenter.comcn_violationhalding where id > 9000 limit 3;"
data = pd.read_sql(query, DATACENTER)

# 将结果写入csv
# data.to_csv("res2.csv", header=0)  # 忽略表头
res = data.to_csv("res2.csv", index=0)
print(res)


def import2mongo(file, db, table, conf):
    """

    :param file:  即将导入的 csv 文件
    :param db:  数据库
    :param table: 表
    :param conf: mongodb的配置项
    :return:
    """
    showposcommand = f"mongoimport --host={conf['host']} --db {db} --collection {table} \
    --type csv --headerline --ignoreBlanks --file {file}"
    # print(showposcommand)

    try:
        p1 = subprocess.Popen(showposcommand, shell=True)
    except Exception as e:
        print(f"fail to import to mongodb, because {e}")
    p1.wait()
    return True


import2mongo("res2.csv", "justtest", "test1", config['mongodb'])
