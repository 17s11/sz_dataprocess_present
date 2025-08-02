# -*- coding: utf-8 -*-
from datetime import datetime
from dateutil.relativedelta import relativedelta

# 获取当前日期
current_date = datetime.now().date()

def deal_index47(connect_db, logger):
    """
    20241025在德勤群潘帅说把46和47都不用要了，用新的规则“最近一个会计年度未分红上限为D” 代替这两个
    CCT_ADJ_047	最近一个会计年度未分红上限为D
    in min
    :param connect_db:
    :param logger:
    :return:
    """
    mysql_db = connect_db.connect_mysql()
    mysql_cursor = mysql_db.cursor()

    td_date = current_date - relativedelta(days=1)
    last_year = td_date.year - 1
    before_last_year = td_date.year - 2

    # isn1.np1,
    # isn2.np2,
    # ldai1.bs1,
    # ldai2.bs2,
    select_sql1_bak = f"""
        select 
            if(ldai.bs < isn.np * 0.1,'E',null) cct_adj_047,
            sbi.sec_code
            from istock_db.sec_basic_info sbi
            left join
            (   select org_id,sum(ifnull(net_profit,0)) np from istock_db.income_statement_ns where statement_type_code='HB' 
                and (org_id,ed,announcement_date) in (select org_id,ed,min(announcement_date) from istock_db.income_statement_ns 
                where statement_type_code='HB' and ed in ('{last_year}-12-31','{before_last_year}-12-31') group by org_id,ed) and net_profit is not null group by org_id
            ) isn on sbi.issue_org_id=isn.org_id
            left join 
            (   select org_id,sum(ifnull(benchmark_share,0)) bs from istock_db.lc_dividend_and_increase where dividend_year in ('{last_year}-12-31','{before_last_year}-12-31') 
                and benchmark_share is not null group by org_id
            ) ldai on sbi.issue_org_id=ldai.org_id 
            where sbi.sec_type='A股' and isn.org_id is not null and ldai.org_id is not null;
    """

    select_sql1 = f"""
        select 
            li.cct_adj_047,
            sbi.sec_code
        from istock_db.sec_basic_info sbi
        left join
        (select org_id,'D' cct_adj_047 from istock_db.lc_dividend_and_increase 
        where dividend_year='{last_year}-12-31' and ifnull(benchmark_share,0)=0 ) li
        on sbi.issue_org_id=li.org_id 
        where li.org_id is not null
            and sbi.sec_type='A股';
    """
    logger.info('cct_adj_047开始处理')
    mysql_cursor.execute(select_sql1)
    if mysql_cursor.rowcount == 0:
        logger.info('cct_adj_047没有符合的数据')
        return None
    data = mysql_cursor.fetchall()
    update_sql = ('update istock_db.app_index set cct_adj_047=%s where sec_code=%s;')
    mysql_cursor.execute('start transaction;')
    try:
        mysql_cursor.executemany(update_sql, data)
        mysql_db.commit()
        logger.info('cct_adj_047全部更新完成')
    except Exception as e:
        mysql_db.rollback()
        logger.info(f'cct_adj_047更新失败:{e}')

    mysql_cursor.close()
    mysql_db.close()


def run(connect_db, logger):
    deal_index47(connect_db, logger)
    deal_index48(connect_db, logger)
    deal_index49(connect_db, logger)


if __name__ == '__main__':
    from logging_config import setup_logging
    from db import connect_db

    logger = setup_logging()
    deal_index47(connect_db, logger)