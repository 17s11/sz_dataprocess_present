# -*- coding: utf-8 -*-
import os
from loguru import logger

def setup_logging():
    # 本地测试减少日志占用空间
    if os.name == 'nt':
        # 配置日志文件轮转，10M轮转一次，2个副本
        log_dir = os.path.join(os.path.dirname(__file__), 'logs')
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_file = os.path.join(log_dir, 'run.log')
        # logger.add("run.log", rotation="1 day", retention="10 days", compression="zip")
        logger.add(log_file, rotation=10 * 1024 * 1024, retention=2, enqueue=True)
        return logger
    else:
        # 服务器配置日志文件轮转，500M轮转一次，10个副本
        log_dir = os.path.join(os.path.dirname(__file__), 'logs')
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_file = os.path.join(log_dir, 'run.log')
        # logger.add("run.log", rotation="1 day", retention="10 days", compression="zip")
        logger.add(log_file, rotation=500 * 1024 * 1024, retention=10, enqueue=True)
        return logger