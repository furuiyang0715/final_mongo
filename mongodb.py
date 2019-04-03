import configparser

import pymongo
import urllib.parse
import logging
import sys

from pymongo.errors import CollectionInvalid


class SysException(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class MyMongoDB:

    mdb = None

    def __init__(self, conf, logger):
        self.logger = logger
        self.conf = conf
        try:
            password = urllib.parse.quote(conf['password'])
        except Exception:
            raise

        if conf['user'] == '':
            conn_string = 'mongodb://' + \
                            conf['host'] + ':' + \
                            conf['port'] + '/'
        else:
            conn_string = 'mongodb://' + \
                            conf['user'] + ':' + \
                            password + '@' + \
                            conf['host'] + ':' + \
                            conf['port'] + '/'
        try:
            self.mdb = pymongo.MongoClient(conn_string, connect=False)
        except Exception:
            raise

    def get_db(self, db_name):
        try:
            db = self.mdb[db_name]
        except:
            try:
                db = self.mdb.get_database(db_name)
            except Exception:
                raise SysException

        return db

    def get_coll(self, coll_name, db_name):
        db = None

        try:
            db = self.get_db(db_name)
        except Exception:
            SysException(e)

        try:
            db.create_collection(coll_name)
        except CollectionInvalid:
            pass
        except Exception:
            raise

        coll = db[coll_name]

        return coll

    def get_last_id(self, table):
        """
        获取上一次记录的更新到的 id 位置
        :param table:
        :return:
        """
        util_name = self.conf['util']
        db_name = self.conf['databases']
        util_coll = self.get_coll(util_name, db_name)

        # {"table": table, "last_id": -1, "timestamp": 23456}
        try:
            last_id = list(util_coll.find({"table": table}, {"last_id": 1}))
        except Exception:
            raise

        last_id = last_id[0].get("last_id") if last_id else -1

        return last_id

    def get_last_sync_time(self, table):
        """
        获取上一次的同步时间
        :param table:
        :return:
        """
        util_name = self.conf['util']
        db_name = self.conf['databases']
        util_coll = self.get_coll(util_name, db_name)

        # {"table": table, "last_id": -1, "timestamp": 23456}
        try:
            last_time = list(util_coll.find({"table": table}, {"timestamp": 1}))
        except Exception:
            raise

        last_time = last_time[0].get("last_time") if last_time else None

        return last_time

    def flash_time(self, table, time):
        """
        将某个表的同步时间刷新
        :param table:
        :param time:
        :return:
        """
        util_name = self.conf['util']
        db_name = self.conf['databases']
        util_coll = self.get_coll(util_name, db_name)
        # 判断是否存在 util_coll
        exist = list(util_coll.find({"table": table}))

        if not exist:
            data = {"table": table, "last_id": -1, "timestamp": None}
            util_coll.insert_one(data)
            # print("被创建了 ")

        # {"table": table, "last_id": -1, "timestamp": 23456}
        try:
            update = util_coll.update({"table": table}, {"$set": {"timestamp": time}})
        except Exception:
            raise

        # res = list(util_coll.find({"table": table}))
        # print(res)
        return update

    def flash_id(self, table, new_id):
        """
        将最新的更新位置 id 刷新到数据库中
        :param table:
        :param new_id:
        :return:
        """
        util_name = self.conf['util']
        db_name = self.conf['databases']
        util_coll = self.get_coll(util_name, db_name)
        # 判断是否存在 util_coll
        exist = list(util_coll.find({"table": table}))

        if not exist:
            data = {"table": table, "last_id": -1, "timestamp": None}
            util_coll.insert_one(data)
            # print("被创建了 ")

        # {"table": table, "last_id": -1, "timestamp": 23456}
        try:
            update = util_coll.update({"table": table}, {"$set": {"last_id": new_id}})
        except Exception:
            raise

        # res = list(util_coll.find({"table": table}))
        # print(res)
        return update

    def delete_ids(self, table, del_list):
        """
        删除 id 列表里面的记录
        :param table:
        :param del_list:
        :return:
        """
        db = self.conf['databases']
        coll = self.get_coll(table, db)
        res = coll.delete_many({"id": {"$in": del_list}})
        return res


# if __name__ == "__main__":
#     logger = logging.getLogger(__name__)
#     config = configparser.ConfigParser()
#     config.read('conf/config.ini')
#     conf = config['mongodb']
#     mongo = MyMongoDB(conf, logger)
#     table = "futures_basic"
#     ids = [1, 3, 4, 6, 7]
#     res = mongo.delete_ids(table, ids)
#     print(res)
