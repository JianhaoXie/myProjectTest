#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine
from db_config import *
import pymysql

def dataToMysql(sqlTable,excelTable):
    # 写入数据库
    df = pd.read_excel(excelTable,sheet_name='all')

    engine = create_engine(conn_data)

    df.to_sql(name = sqlTable,con = engine,if_exists = 'append',index = False,index_label = False)

def createTable():
    '''
    mysql 建表
    :return: 无
    '''
    conn = pymysql.connect(host=host, port=port, user=user, password=password, db=db, charset='utf8')
    cur = conn.cursor()
    sql = '''
    create table %s(
    `ID` int auto_increment primary key not null comment '自增长ID',
    `YEAR` int(4) NOT NULL comment '年份',
    `S` float NOT NULL comment '商品房供应量(万平方米)',
    `POP` float NOT NULL comment '人口(万人)',
    `GDP` float NOT NULL comment '地区生产总值(亿元)', 
    `INC` float NOT NULL comment '农村人均可支配收入(元)',
    `D` float NOT NUll comment '商品房需求量(万平方米)',
    `RATE` float NOT NULL comment '供需比',
    `DIST` varchar(10) NOT NULL comment '区域,行政街道',
    `COMP` float default 0 comment '竣工面积(万平方米)',
    `AMT` float default 0 comment '住宅销售额(万元)',
    `PRICE` float default 0 comment '商品房销售均价(元/平方米)'
    )comment='房屋供需比';
    
    create table %s (
    `ID` int auto_increment primary key not null comment '自增长ID',
    `YEAR` int(4) NOT NULL comment '年份',
    `POP` float NOT NULL comment '户籍人口(万人)',
    `WS` float NOT NULL comment '待售面积(万平方米)',
    `HAMT` float NOT NULL comment '待售面积/住宅(万平方米)',
    `PPRICE` float NOT NULL comment '直接预测房价(元/平方米)',
    `PRICE` float NOT NULL comment '房价(元/平方米)',
    `THN` float NOT NULL comment '总户数(万户)',
    `GDP` float NOT NULL comment '生产总值(万元)',
    `PGDP` float NOT NULL comment '人均生产总值(元)',
    `DINC` float NOT NULL comment '人均可支配收入(元)',
    `UDINC` float NOT NULL comment '城镇人均可支配收入(元)',
    `RDINC` float NOT NULL comment '农村人均可支配收入(万元)',
    `LINV` float NOT NULL comment '房地产开发商投资额(万元)',
    `HLINV` float NOT NULL comment '方底层开发商投资额/住宅(万元)',
    `HSALES` float NOT NULL comment '商品房销售面积(万平方米)',
    `SALES` float NOT NULL comment '住宅销售面积(万平方米)',
    `R_THN` float NOT NULL comment 'peason相关系数_总户数',
    `R_POP` float NOT NULL comment 'peason相关系数_户籍人口',
    `R_GDP` float NOT NULL comment 'peason相关系数_GDP',
    `R_PGDP` float NOT NULL comment 'peason相关系数_人均生产总值',
    `R_DINC` float NOT NULL comment 'peason相关系数_人均可支配收入',
    `R_UDINC` float NOT NULL comment 'peason相关系数_城镇人均可支配收入',
    `R_RDINC` float NOT NULL comment 'peason相关系数_农村人均可支配收入',
    `R_LINV` float NOT NULL comment 'peason相关系数_房地产开发商投资额',
    `R_HLINV` float NOT NULL comment 'peason相关系数_房地产开发商投资额',
    `R_HSALES` float NOT NULL comment 'peason相关系数_商品房销售面积',
    `R_SALES` float NOT NULL comment 'peason相关系数_住宅销售面积',
    `P_THN` float NOT NULL comment 'p值_总户数',
    `P_POP` float NOT NULL comment 'p值_户籍人口',
    `P_GDP` float NOT NULL comment 'p值_GDP',
    `P_PGDP` float NOT NULL comment 'p值_人均生产总值',
    `P_DINC` float NOT NULL comment 'p值_人均可支配收入',
    `P_UDINC` float NOT NULL comment 'p值_城镇人均可支配收入',
    `P_RDINC` float NOT NULL comment 'p值_农村人均可支配收入',
    `P_LINV` float NOT NULL comment 'p值_房地产开发商投资额',
    `P_HLINV` float NOT NULL comment 'p值_房地产开发商投资额',
    `P_HSALES` float NOT NULL comment 'p值_商品房销售面积',
    `P_SALES` float NOT NULL comment 'p值_住宅销售面积',
    `DIST` varchar(10) NOT NULL comment '地区'
    )comment '诸暨房价预测';
    ''' % (table1,table2)
    cur.execute(sql)
    conn.close()
    conn.commit()



if __name__ == '__main__':
    # createTable()
    dataToMysql(table1,'rate.xlsx')
    dataToMysql(table2,'price.xlsx')
    # pass