# -*- coding: utf-8 -*-
import os
import sys
import time

file_path = os.path.abspath(__file__)
project_path = os.path.dirname(file_path)
sys.path.append(project_path)

from prepare_options import check
from prepare_options import five_class
from cct_adj_index import cct_adj_g1
from cct_adj_index import cct_adj_g2
from cct_adj_index import cct_adj_g3
from cct_adj_index import cct_adj_g4
from last_options import merge_index
from last_options import alter_flag

from logging_config import setup_logging

from db import connect_db

logger = setup_logging()

if __name__ == '__main__':
    try:
        logger.info('main start')
        if check.check_td_date(connect_db, logger) == 0:
            logger.info('昨天不是交易日，不处理数据，退出主程序')
            sys.exit(0)
        while True:
            if check.check_flag(connect_db, logger):
                break
            time.sleep(60)
        cct_adj_g1.run(connect_db, logger)
        # cct_adj_g1 中跟诚通公有的指标都在里面，完成后就可以修改公有指标完成标记位了
        alter_flag.alter_flag(connect_db, logger)
        cct_adj_g2.run(connect_db, logger)
        cct_adj_g3.run(connect_db, logger)
        cct_adj_g4.run(connect_db, logger)
        five_class.set_five_class(connect_db, logger) # 初始化五级分类
        merge_index.merge_sz_cct_adj(connect_db, logger)
        logger.info('main end')
    except Exception as e:
        logger.error('main failed')
        logger.error(e)