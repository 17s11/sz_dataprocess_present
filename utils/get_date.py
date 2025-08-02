# -*- coding: utf-8 -*-
from datetime import datetime
from dateutil.relativedelta import relativedelta

# 获取当前日期
current_date = datetime.now().date()

# 计算365天前的日期,限制查询数据的时间，避免查询数据过多，占用宿主机资源
before_365_date = current_date - relativedelta(days=365)
def get_last_td(mysql_cursor):
    select_sql1 = 'select distinct td_date from istock_db.shsz_stock_daily_quotation where td_date>=%s order by td_date desc limit 1;'
    mysql_cursor.execute(select_sql1, before_365_date)
    if mysql_cursor.rowcount == 0:
        return None
    else:
        return mysql_cursor.fetchone()[0]