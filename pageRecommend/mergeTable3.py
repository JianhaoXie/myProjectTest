#!/usr/bin/python3
# -*- coding: utf-8 -*-


import pymysql
import pandas as pd
import numpy as np
from db_config import *

'''
合并数据 用户表,对象表,行为表数据
'''

# 连接数据库


def AGE(value):
    if not value or len(value) < 15:
        return None
    if len(value) == 15:
        return 119 - int(str(value)[6:8])
    birth = str(value)[6:10]
    return 2019 - int(birth)

def ARE1(value):
    if not value or (len(value) != 15 and len(value) != 18):
        return None
    return int(str(value)[:2])

def ARE2(value):
    if not value or (len(value) != 15 and len(value) != 18):
        return None
    return int(str(value)[2:4])

def ARE3(value):
    if not value or (len(value) != 15 and len(value) != 18):
        return None
    return int(str(value)[4:6])

def userTable():
    '''
    C_USER 表处理
    :return: user表的dataframe形式
    '''
    conn = pymysql.connect(host=host, port=port, user=user, password=password, db=db, charset='utf8')
    cur = conn.cursor()
    sql = """select `ID`,`WORKNO`,`SEX`,`ORG_ID`,`POSITION_ID` from C_USER;"""
    cur.execute(sql)
    results = cur.fetchall()

    columns = ['USER_ID','WORKNO','SEX','ORG_ID','POSITION_ID']

    user_df = pd.DataFrame(list(results),columns=columns)

    # 替换SEX未知字段及空白字段为0
    user_df['SEX'] = user_df['SEX'].replace({3:0})
    user_df['SEX'] = user_df['SEX'].fillna(0)

    # 将身份证号分解成几个有用字段
    user_df['AGE'] = user_df['WORKNO'].apply(AGE)
    user_df['ARE1'] = user_df['WORKNO'].apply(ARE1)
    user_df['ARE2'] = user_df['WORKNO'].apply(ARE2)
    user_df['ARE3'] = user_df['WORKNO'].apply(ARE3)

    user_df = user_df.fillna(-1)
    del user_df['WORKNO']
    cur.close()
    conn.close()
    return user_df

def objectTable():
    '''
    对象表处理
    :return: 对象表的dataframe形式
    '''
    conn = pymysql.connect(host=host, port=port, user=user, password=password, db=db, charset='utf8')
    cur = conn.cursor()
    sql = '''select * from C_OBJECT;'''
    cur.execute(sql)
    results = cur.fetchall()
    object_df = pd.DataFrame(list(results), columns=['OBJECT_ID', 'RES_NAME'])

    cur.close()
    conn.close()

    return object_df

def accesslogTable():
    '''
    行为表处理
    :return:  行为表的dataframe形式
    '''
    conn = pymysql.connect(host=host, port=port, user=user, password=password, db=db, charset='utf8')
    cur = conn.cursor()

    sql = '''select RES_NAME,USER_ID from C_ACCESSLOG;'''
    cur.execute(sql)
    results = cur.fetchall()
    accesslog_df = pd.DataFrame(list(results), columns=['RES_NAME','USER_ID'])

    cur.close()
    conn.close()

    return accesslog_df


def mergeTable():
    '''
    合并三表
    :return: 合并完成后的dataframe
    '''
    user_df = userTable()
    object_df = objectTable()
    accesslog_df = accesslogTable()

    #  合并对象表和行为表为df
    df = pd.merge(accesslog_df,object_df,on='RES_NAME')

    # 继续合并用户信息
    df = pd.merge(df,user_df,on='USER_ID')

    del df['RES_NAME']

    return df


dataframe = mergeTable()
user_df = userTable()

if __name__ == '__main__':
    # userTable()
    # objectTable()
    # accesslogTable()
    # df = mergeTable()
    print(user_df)