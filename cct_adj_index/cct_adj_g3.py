# -*- coding: utf-8 -*-
from datetime import datetime
from dateutil.relativedelta import relativedelta

# 获取当前日期
current_date = datetime.now().date()

def deal_index20v1_v2(connect_db, logger):
    """
    CCT_ADJ_020V1	连续20个交易日的市值低于20亿元且在沪深创业/科创板上市上限为D
    CCT_ADJ_020V2	连续20个交易日的市值低于20亿元且在北交所上市上限为D
    in 2min
    :param connect_db:
    :param logger:
    :return:
    """
    mysql_db = connect_db.connect_mysql()
    mysql_cursor = mysql_db.cursor()

    select_sql1 = """
        select 
            if(sbi.td_mkt in ("上交所","深交所") and sbi.listed_board_name in ("科创板", "创业板"),"D",null) cct_adj_020v1,
            if(sbi.td_mkt="北交所","D",null) cct_adj_020v2,
            sbi.sec_code
        from istock_db.sec_basic_info sbi 
        left join istock_db.mid_index mi on sbi.sec_code COLLATE utf8mb4_unicode_ci =mi.sec_code
        where mi.mid_005>=20
        and sbi.sec_type="A股" 
        and (sbi.td_mkt not in ("上交所","深交所") or sbi.listed_board_name!='主板');
    """
    logger.info('cct_adj_020v1_v2开始处理')
    mysql_cursor.execute(select_sql1)
    if mysql_cursor.rowcount == 0:
        logger.info('cct_adj_020v1_v2没有符合的数据')
        return None
    data = mysql_cursor.fetchall()
    update_sql = ('update istock_db.app_index set cct_adj_020v1=%s,cct_adj_020v2=%s where sec_code=%s;')
    mysql_cursor.execute('start transaction;')
    try:
        mysql_cursor.executemany(update_sql, data)
        mysql_db.commit()
        logger.info('cct_adj_020v1_v2全部更新完成')
    except Exception as e:
        mysql_db.rollback()
        logger.info(f'cct_adj_020v1_v2更新失败:{e}')

    mysql_cursor.close()
    mysql_db.close()



def run(connect_db, logger):
    deal_index20v1_v2(connect_db, logger)
    deal_index21v1_v2(connect_db, logger)
    deal_index23v1(connect_db, logger)
    deal_index24v1(connect_db, logger)
    deal_index26v1(connect_db, logger)
    deal_index40_41(connect_db, logger)
    deal_index42_45(connect_db, logger)
    # deal_index46(connect_db, logger)


if __name__ == '__main__':
    from logging_config import setup_logging
    from db import connect_db

    logger = setup_logging()
    deal_index40_41(connect_db, logger)
    deal_index42_45(connect_db, logger)