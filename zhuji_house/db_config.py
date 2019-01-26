#!/usr/bin/python3
# -*- coding: utf-8 -*-

'''
配置文件
'''


# 数据库配置
host = '121.40.214.176'
port = 3306
user = 'fline'
password = '000000'
db = 'zjbigdata'
# 五个镇的表及诸暨全市的数据表
table1 = 'supply_ratio'

# 诸暨全市房价
table2 = 'house_price_forecast'

conn_data = "mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8".format(user, password, host, port, db)
