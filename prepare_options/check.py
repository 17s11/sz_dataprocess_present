# -*- coding: utf-8 -*-
from datetime import datetime

from dateutil.relativedelta import relativedelta


def check_flag(connect_db, logger):
    """
    检查中间层指标是否计算完成
    :param connect_db:
    :param logger:
    :return: 0：中间层指标计算未完成，1：中间层指标计算完成
    """
    mysql_db = connect_db.connect_mysql()
    mysql_cursor = mysql_db.cursor()
    select_sql = 'select complete_flag from istock_db.mid_index where complete_flag=1 limit 1;'
    mysql_cursor.execute(select_sql)
    if mysql_cursor.rowcount == 0:
        flag =  0
    else:
        flag = 1

    mysql_cursor.close()
    mysql_db.close()
    logger.info(f'检测中间层指标是否计算完成标记位为:{flag}')
    return  flag


if __name__ == '__main__':
    from logging_config import setup_logging
    from db import connect_db
    logger = setup_logging()
    logger.info(check_flag(connect_db, logger))