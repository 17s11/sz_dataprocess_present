# -*- coding: utf-8 -*-
from datetime import datetime
from dateutil.relativedelta import relativedelta

# 获取当前日期
current_date = datetime.now().date()

# 计算365天前的日期,限制查询数据的时间，避免查询数据过多，占用宿主机资源
before_365_date = current_date - relativedelta(days=365)


def deal_index15v1_v4(connect_db, logger):
    """
    14和15的很像，后面可以整合到一起（包括V）
    CCT_ADJ_015V1	连续90个交易日累计成交量低于600万股且在沪深主板上市上限为D
    CCT_ADJ_015V2	连续90个交易日累计成交量低于200万股且在沪深创业/科创板上市上限为D
    CCT_ADJ_015V3	连续90个交易日累计成交量低于100万股且在北交所上市上限为D
    CCT_ADJ_015V4	连续90个交易日累计成交量低于200万股且在北交所上市上限为E
    :param connect_db:
    :param logger:
    :return:
    """
    mysql_db = connect_db.connect_mysql()
    mysql_cursor = mysql_db.cursor()

    select_sql = """
        select 
            if(sbi.td_mkt in ("上交所","深交所") and sbi.listed_board_name="主板","D",null) cct_015v1,
            if(mi.mid_001v1<2000000 and sbi.td_mkt in ("上交所","深交所") and sbi.listed_board_name in ("创业板","科创板"),"D",null) cct_015v2,
            if(mi.mid_001v1<1000000 and sbi.td_mkt="北交所","D",null) cct_015v3,
            if(mi.mid_001v1<2000000 and sbi.td_mkt="北交所","E",null) cct_015v4,
            mi.sec_code 
        from istock_db.mid_index mi 
        left join istock_db.sec_basic_info sbi on mi.sec_code COLLATE utf8mb4_unicode_ci = sbi.sec_code 
        where mi.mid_001v1<6000000 
            and sbi.sec_type="A股";
    """
    logger.info('cct_adj_015v1_v4开始处理')
    mysql_cursor.execute(select_sql)
    if mysql_cursor.rowcount == 0:
        logger.info('cct_adj_015v1_v4没有符合的消息')
        return None
    data = mysql_cursor.fetchall()
    update_sql = ('update istock_db.app_index set cct_adj_015v1=%s,cct_adj_015v2=%s,'
                  'cct_adj_015v3=%s,cct_adj_015v4=%s where sec_code=%s;')
    mysql_cursor.execute('start transaction;')
    try:
        mysql_cursor.executemany(update_sql, data)
        mysql_db.commit()
        logger.info('cct_adj_015v1_v4全部更新完成')
    except Exception as e:
        mysql_db.rollback()
        logger.info(f'cct_adj_015v1_v4更新失败:{e}')

    mysql_cursor.close()
    mysql_db.close()


def deal_index16v1and17v1(connect_db, logger):
    """
    CCT_ADJ_016V1	连续20个交易日的股价低于2元且在沪深上市上限为D
    CCT_ADJ_017V1	连续20个交易日的股价低于1.5元且在沪深上市上限为E
    :param connect_db:
    :param logger:
    :return:
    """
    mysql_db = connect_db.connect_mysql()
    mysql_cursor = mysql_db.cursor()

    select_sql = """
        select if(mid_004>=20,"D",null) cct_adj_016v1,
                if(mid_003>=20,"E",null) cct_adj_017v1,
                sec_code 
        from istock_db.mid_index where (mid_004>=20 or mid_003>=20) 
        and sec_code COLLATE utf8mb4_unicode_ci in (select sec_code from istock_db.sec_basic_info 
        where sec_type="A股" and td_mkt in ("上交所","深交所"));
    """
    logger.info('cct_adj_016v1and17v1开始处理')
    mysql_cursor.execute(select_sql)
    if mysql_cursor.rowcount == 0:
        logger.info('cct_adj_016v1and17v1没有符合的消息')
        return None
    data = mysql_cursor.fetchall()
    update_sql = 'update istock_db.app_index set cct_adj_016v1=%s,cct_adj_017v1=%s where sec_code=%s;'
    mysql_cursor.execute('start transaction;')
    try:
        mysql_cursor.executemany(update_sql, data)
        mysql_db.commit()
        logger.info('cct_adj_016v1and17v1全部更新完成')
    except Exception as e:
        mysql_db.rollback()
        logger.info(f'cct_adj_016v1and17v1更新失败:{e}')

    mysql_cursor.close()
    mysql_db.close()


def run(connect_db, logger):
    deal_index34(connect_db, logger)
    deal_index35_36(connect_db, logger)
    deal_index37_38(connect_db, logger)
    deal_index10v1(connect_db, logger)
    deal_index11v1and_39(connect_db, logger)
    deal_index13v1(connect_db, logger)
    deal_index14v1_v4(connect_db, logger)
    deal_index15v1_v4(connect_db, logger)
    deal_index16v1and17v1(connect_db, logger)
    deal_index17v2_v3(connect_db, logger)



if __name__ == '__main__':
    from logging_config import setup_logging
    from db import connect_db

    logger = setup_logging()
    deal_index10v1(connect_db, logger)