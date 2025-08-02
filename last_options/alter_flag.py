# -*- coding: utf-8 -*-
import time

def alter_flag(connect_db, logger):
    """
    中间层指标计算完成后更新标志位，告知应用层指标代码是否可以开始计算应用层指标
    :param connect_db:
    :param logger:
    :return:
    """
    mysql_db = connect_db.connect_mysql()
    mysql_cursor = mysql_db.cursor()

    update_sql = 'update istock_db.app_index set complete_ct_cct_flag=1;'
    mysql_cursor.execute('start transaction;')
    while True:
        try:
            mysql_cursor.execute(update_sql)
            mysql_db.commit()
            logger.info('更新app_index sz和ct公有指标完成标志位成功')
            break
        except Exception as e:
            mysql_db.rollback()
            logger.info(f'更新app_index sz和ct公有指标完成标志位失败 30s后重试:{e}')
            time.sleep(30)

    mysql_cursor.close()
    mysql_db.close()
