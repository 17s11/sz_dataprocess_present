# -*- coding: utf-8 -*-
import os
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta

current_path = os.path.dirname(__file__)
project_path = os.path.dirname(current_path)
sys.path.append(project_path + '/utils')

from utils.get_date import get_last_td

# 获取当前日期
current_date = datetime.now().date()

# 计算365天前的日期,限制查询数据的时间，避免查询数据过多，占用宿主机资源
before_365_date = current_date - relativedelta(days=365)

def deal_index1v1(connect_db, logger):
    """
    CCT_ADJ_001V1	上市状态为ST/*ST上限为E
    :param connect_db:
    :param logger:
    :return:
    """
    mysql_db = connect_db.connect_mysql()
    mysql_cursor = mysql_db.cursor()

    select_sql1 = 'select "E",sec_code from istock_db.sec_basic_info where listed_status in ("ST","*ST") and sec_type="A股";'
    logger.info('CCT_ADJ_001V1 开始处理')
    mysql_cursor.execute(select_sql1)
    if mysql_cursor.rowcount == 0:
        return None
    data = mysql_cursor.fetchall()
    update_sql = 'update istock_db.app_index set cct_adj_001v1=%s where sec_code=%s;'
    mysql_cursor.execute('start transaction;')
    try:
        mysql_cursor.executemany(update_sql, data)
        mysql_db.commit()
        logger.info('CCT_ADJ_001V1指标全部更新完成')
    except Exception as e:
        mysql_db.rollback()
        logger.info(f'CCT_ADJ_001V1更新失败:{e}')

    mysql_cursor.close()
    mysql_db.close()


def run(connect_db, logger):
    deal_index1v1(connect_db, logger)
    deal_index7(connect_db, logger)
    deal_index8(connect_db, logger)
    deal_index9(connect_db, logger)
    deal_index12(connect_db, logger)
    deal_index18_19(connect_db, logger)
    deal_index22(connect_db, logger)
    # deal_index25(connect_db, logger) # 2025-01-20 德勤会议沟通这个指标去掉，因为024就是包含了025（025中的取消就是024的不符合条件的）
    deal_index27(connect_db, logger)  # CCT_ADJ的指标7、8、9、12、18、19、22、25、27在计算最后一个指标时修改标记位cct_ct_flag=1,因为这些指标山证和诚通公有的指标，防止诚通取最严重等级的时候山证这边的公有指标没有计算完
    deal_index33(connect_db, logger)


if __name__ == '__main__':
    from logging_config import setup_logging
    from db import connect_db

    logger = setup_logging()
    deal_index22(connect_db, logger)