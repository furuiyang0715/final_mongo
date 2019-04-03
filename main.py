import configparser
import time
import requests
import json
import sys

import logging.config
from importlib import util
from apscheduler.schedulers.blocking import BlockingScheduler

from check_sqlbinlog import gen_binloginfo
from daemon import Daemon
from justmysql import MySql
from mongodb import MyMongoDB
from mysql_csv import load_data
from sync_tables import tables

config = configparser.ConfigParser()
config.read('conf/config.ini')


def task(logger):
    # 基础配置项
    # config = configparser.ConfigParser()
    # config.read('conf/config.ini')

    # mysql 的配置项
    conf = config['mysql']

    # mongodb 的配置项
    m_conf = config['mongodb']

    # 日志
    # logger = logging.getLogger(__name__)

    # 创建一个 mongo 连接
    mongo = MyMongoDB(m_conf, logger)

    # 创建一个 mysql 连接
    sql = MySql(conf, logger)

    # 获取所需的数据库表
    tables_ = tables[:8]
    logger.info(tables_)

    # 获取所需要的数据库
    db_ = [conf['databases']]
    logger.info(db_)

    for db in db_:
        for table in tables_:
            # 获取更新的起始位置 即上一次更新到的 id
            start = mongo.get_last_id(table)
            logger.info(f"start: {start}")

            # 获取上一次更新的时间
            last_time = mongo.get_last_sync_time(table)
            logger.info(f"last_time: {last_time}")

            # 获取本次最新到的 id
            end = sql.get_now_id(table).get(table)
            logger.info(f"end: {end}")

            # 更新条件
            if start > end:
                logger.warning("some thing has been wrong.")
                raise SystemError(f"start_id > end_id, in table {table}")
            elif start == end:
                logger.info("no data need to sync.")
            else:
                # 本次更新的时间点
                new_time = time.time()

                # 进行更新 (start, end] 左开右闭区间
                load_data(conf, m_conf, db, table, start, end)

                # 将本次更新的时间点写入
                res = mongo.flash_time(table, new_time)
                logger.info(f"flash_time: {res}")

                # 将本地更新到的 id 位置写入
                res2 = mongo.flash_id(table, end)
                logger.info(f"flash_id: {res2}")

                # 查询出两个时间点之间的删除操作 进行删除
                del_list = gen_binloginfo(conf, last_time, new_time, [db], [table])
                logger.info(f"del_list: {del_list}")

                # 删除mongo里面的记录
                res3 = mongo.delete_ids(table, del_list)
                logger.info(f"delete_ids: {res3}")


class LoggerWriter:
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level

    def write(self, message):
        if message != '\n':
            self.logger.log(self.level, message)

    def flush(self):
        return True


class MyMongoDaemon(Daemon):
    def run(self):
        sys.stderr = self.log_err

        try:
            util.find_spec('setproctitle')
            self.setproctitle = True
            import setproctitle
            setproctitle.setproctitle('sync')
        except ImportError:
            self.setproctitle = False

        self.logger.info("Running. ")
        self.dummy_sched()

        self.scheduler()

    def scheduler(self):
        sched = BlockingScheduler()
        try:
            sched.add_job(self.dummy_sched, 'interval', minutes=20)
            sched.start()
        except Exception as e:
            self.logger.error(f'Cannot start scheduler. Error: {e}')
            sys.exit(1)

    def poke_one(self):
        """开始工作前戳一次"""

        url = "http://172.17.0.1:9999/metrics"
        d = {
            "container_id": "0002",
            "instance": "sync_exporter",
            "job": "sync_exporter",
            "name": "sync_exporter"
        }
        code = ''
        for i in range(10):
            try:
                res = requests.post(url, data=json.dumps(d), timeout=0.5)
                code = res.status_code
            except Exception:
                break
            if code == 200:
                break
        self.logger.info(f"poking 0002, code = {code}")

    def dummy_sched(self):
        self.poke_one()
        try:
            task(self.logger)
        except Exception as e:
            self.logger.warning(f"task fail, {e}")
            sys.exit(1)

    def write_pid(self, pid):
        open(self.pidfile, 'a+').write("{}\n".format(pid))


if __name__ == "__main__":
    logging.config.fileConfig('conf/logging.conf')
    logger = logging.getLogger('root')

    pid_file = config['log']['pidfile']
    logger.info(pid_file)
    log_err = LoggerWriter(logger, logging.ERROR)

    worker = MyMongoDaemon(pidfile=pid_file, log_err=log_err)

    if len(sys.argv) >= 2:
        if 'start' == sys.argv[1]:
            worker.start()
        elif 'stop' == sys.argv[1]:
            worker.stop()
        elif 'restart' == sys.argv[1]:
            worker.restart()
        elif 'status' == sys.argv[1]:
            worker.status()
        else:
            sys.stderr.write("Unknown command\n")
            sys.exit(2)
        sys.exit(0)
    else:
        sys.stderr.write("usage: %s start|stop|restart\n" % sys.argv[0])
        sys.exit(2)
