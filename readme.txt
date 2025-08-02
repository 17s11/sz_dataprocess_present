山证的指标
1.数据库配置文件：conf/config.json
2.应用层指标处理目录：cct_adj_index
3.中间层指标处理目录：mid_index（目前移植到中间层指标部署任务中，应用层指标不在单独计算中间层指标）
4.检测新股票入库目录：prepare_options
5.最后的取等级最高合并等级、转移数据到德勤表牡蛎：last_options
6.主程序入口函数：mian.py (启动项目时直接启动主函数main.py即可)