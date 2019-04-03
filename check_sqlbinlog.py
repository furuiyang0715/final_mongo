import time

import datetime
import configparser


from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)


def mysql_stream(conf, timestamp, dbs, tables):
    """
    查找某个数据库中某些表在一个固定的时间戳忠之后的修改和删除的数据的id
    :param conf:  mysql 相关配置
    :param timestamp: 时间戳
    :return:
    """

    del_list = list()
    update_list = list()
    add_list = list()

    mysql_settings = {
        "host": conf['host'],
        "port": conf.getint('port'),
        "user": conf['user'],
        "passwd": conf['password']
    }

    # mysql_settings = {'host': '127.0.0.1', 'port': 3306, 'user': 'root', 'passwd': 'ruiyang'}

    # dbs = ['datacenter']
    # tbs = ["const_secumain"]
    dbs = [dbs] if not isinstance(dbs, list) else dbs
    tables = [tables] if not isinstance(tables, list) else tables

    for db in dbs:
        for table in tables:
            """
            Attributes:
            ctl_connection_settings: Connection settings for cluster holding schema information
            resume_stream: Start for event from position or the latest event of
                           binlog or from older available event
            blocking: Read on stream is blocking
            only_events: Array of allowed events
            ignored_events: Array of ignored events
            log_file: Set replication start log file
            log_pos: Set replication start log pos (resume_stream should be true)
            auto_position: Use master_auto_position gtid to set position
            only_tables: An array with the tables you want to watch (only works
                         in binlog_format ROW)
            ignored_tables: An array with the tables you want to skip
            only_schemas: An array with the schemas you want to watch
            ignored_schemas: An array with the schemas you want to skip
            freeze_schema: If true do not support ALTER TABLE. It's faster.
            skip_to_timestamp: Ignore all events until reaching specified timestamp.
            report_slave: Report slave in SHOW SLAVE HOSTS.
            slave_uuid: Report slave_uuid in SHOW SLAVE HOSTS.
            fail_on_table_metadata_unavailable: Should raise exception if we can't get
                                                table information on row_events
            slave_heartbeat: (seconds) Should master actively send heartbeat on
                             connection. This also reduces traffic in GTID replication
                             on replication resumption (in case many event to skip in
                             binlog). See MASTER_HEARTBEAT_PERIOD in mysql documentation
                             for semantics
            """
            # print(table)
            stream = BinLogStreamReader(connection_settings=mysql_settings,
                                        server_id=conf.getint('slaveid'),
                                        only_events=[
                                                     DeleteRowsEvent,
                                                     WriteRowsEvent,
                                                     UpdateRowsEvent
                                                     ],
                                        # blocking=True,
                                        # resume_stream=resume_stream,
                                        # log_file=log_file,
                                        # log_pos=log_pos,
                                        # 忽略所有的事件 直到达到指定的时间戳
                                        skip_to_timestamp=timestamp,
                                        only_tables=table,   # 只查询当前表的事件
                                        only_schemas=db  # 只查询当前的数据库
                                        )

            for binlogevent in stream:
                # binlogevent.dump()
                schema = "%s" % binlogevent.schema
                table = "%s" % binlogevent.table

                for row in binlogevent.rows:
                    if isinstance(binlogevent, DeleteRowsEvent):
                        vals = row["values"]
                        event_type = 'delete'
                        print(vals.get("id"))
                        del_list.append(vals.get("id"))
                    elif isinstance(binlogevent, UpdateRowsEvent):
                        vals = dict()
                        vals["before"] = row["before_values"]
                        vals["after"] = row["after_values"]
                        event_type = 'update'
                        update_list.append(vals['before'].get("id"))
                    elif isinstance(binlogevent, WriteRowsEvent):
                        vals = row["values"]
                        event_type = 'insert'

            stream.close()
    return del_list, update_list


def gen_binloginfo(conf, t1, t2, dbs, tables):
    """
    计算两个时间点之间的 binlog 日志记录的删除记录
    :param conf:
    :param t1:
    :param t2:
    :param dbs:
    :param tables:
    :return:
    """
    del_list1, update_list1 = mysql_stream(conf, t1, dbs, tables)
    print(del_list1)
    del_list2, update_list2 = mysql_stream(conf, t2, dbs, tables)
    print(del_list2)
    if update_list1 or update_list2:
        raise SystemError("有修改？？")
    del_list = list(set(del_list1) - set(del_list2))
    return del_list


if __name__ == "__main__":
    """用法举例"""
    config = configparser.ConfigParser()
    config.read('conf/config.ini')

    # 获取昨天的时间戳
    yest = datetime.datetime(2019, 4, 1, 14, 0, 0)
    timestamp = time.mktime(yest.timetuple())

    # mysql 配置
    conf = config['mysql']
    # 获取db和table
    _db = conf.get("databases")
    from sync_tables import tables
    _tables = tables[3:4]
    print(_db, "---> ", _tables)

    del_list, update_list = mysql_stream(conf, timestamp, _db, _tables)
    print(del_list, update_list)
