# -*- coding: utf-8 -*-

"""
1.山西证券的调整项合并等级更新到model_st的five_class_result_sx_adj字段（这个字段默认是null值，计算之前默认用five_class_result_adj初始化）
2.应用层指标写到indicator表
"""
def merge_sz_cct_adj(connect_db, logger):


if __name__ == '__main__':
    from logging_config import setup_logging
    from db import connect_db

    logger = setup_logging()
    merge_sz_cct_adj(connect_db, logger)