# -*- coding: utf-8 -*-
from datetime import datetime
from dateutil.relativedelta import relativedelta

# 获取当前日期
current_date = datetime.now().date()



def deal_index_cct10(connect_db, logger):
    """
    MID_CCT_ADJ_010	CCT_ADJ_010布尔值
    :param connect_db:
    :param logger:
    :return:
    """
    mysql_db = connect_db.connect_mysql()
    mysql_cursor = mysql_db.cursor()

    last_year_last_day = datetime(current_date.year - 1, 12, 31)  # 去年的最后一天


    select_sql1 = """
        select 
            1 mid_cct_adj_010,
            sbi.sec_code 
        from istock_db.sec_basic_info sbi 
        left join
        istock_db.mid_index mi on sbi.sec_code COLLATE utf8mb4_unicode_ci =mi.sec_code
        left join
        (select org_id,profit_total_amt,revenue from istock_db.income_statement_ns where ed=%s 
        and statement_type_code='HB' and (org_id, announcement_date) in 
        (select org_id,min(announcement_date) from istock_db.income_statement_ns where ed=%s and statement_type_code='HB' group by org_id)
        and chg_seq=0) isn on sbi.issue_org_id=isn.org_id
        left join 
        (select distinct corp_code from istock_db.calc_financial_indicator 
            where report_period=%s and profit_dnrgal_to_np<0) cfi on sbi.issue_org_id=cfi.corp_code 
        where 
        sbi.sec_type="A股" 
        and sbi.td_mkt in ("上交所","深交所") 
        and sbi.listed_board_name="主板" 
        and (cfi.corp_code is not null or isn.profit_total_amt<0) 
        and isn.revenue<350000000;
    """
    logger.info('mid_cct_adj_010开始处理')
    mysql_cursor.execute(select_sql1, (last_year_last_day, last_year_last_day, last_year_last_day))
    if mysql_cursor.rowcount == 0:
        logger.info('mid_cct_adj_010没有符合的数据')
        return None
    data = mysql_cursor.fetchall()
    update_sql = 'update istock_db.mid_index set mid_cct_adj_010=%s where sec_code=%s;'
    mysql_cursor.execute('start transaction;')
    try:
        mysql_cursor.executemany(update_sql, data)
        mysql_db.commit()
        logger.info('mid_cct_adj_010全部更新完成')
    except Exception as e:
        logger.info(f'mid_cct_adj_010更新失败:{e}')
        mysql_db.rollback()

    mysql_cursor.close()
    mysql_db.close()




def run(connect_db, logger):
    deal_index_cct8_9(connect_db, logger)
    deal_index_cct10(connect_db, logger)
    deal_index_cct11(connect_db, logger)
    deal_index_cct13(connect_db, logger)
    deal_index_cct22(connect_db, logger)
    deal_index_cct23(connect_db, logger)
    deal_index_cct24(connect_db, logger)
    deal_index_cct39(connect_db, logger)


if __name__ == '__main__':
    from logging_config import setup_logging
    from db import connect_db

    logger = setup_logging()
    deal_index_cct22(connect_db, logger)