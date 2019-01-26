#!/usr/bin/python3
# -*- coding: utf-8 -*-

import time
import pandas as pd
import pymysql
import math
from sqlalchemy import create_engine
from scipy import stats

from db_config import *
from lstm_predict import lstm_model
from arma_predict import arma_model


def commitDfToMysql(df,table):
    # 这里一定要写成mysql+pymysql，不要写成mysql+mysqldb
    engine = create_engine(conn_data)
    df.to_sql(name=table, con=engine, if_exists='append', index=False, index_label=False)


def readTable1():
    '''
    连接数据库,获取供需比数据,返回dataframe形似
    :return: dataframe
    '''
    conn = pymysql.connect(host=host,port=port,user=user,password=password,db=db,charset='utf8')
    cur = conn.cursor()

    sql = "select year,s,pop,gdp,inc,d,rate,dist,comp,amt,price from %s" % table1
    cur.execute(sql)
    results = cur.fetchall()
    df = pd.DataFrame(list(results),columns=['year','s','pop','gdp','inc','d','rate','dist','comp','amt','price'])
    conn.close()
    return df


def readTable2():
    '''
    连接数据库,获取诸暨房价数据数据,返回dataframe形似
    :return: dataframe
    '''
    conn = pymysql.connect(host=host, port=port, user=user, password=password, db=db, charset='utf8')
    cur = conn.cursor()

    sql = "select YEAR,POP,WS,HAMT,PPRICE,PRICE,THN,GDP,PGDP,DINC,UDINC,RDINC,LINV,HLINV,HSALES,SALES,R_THN,R_POP," \
          "R_GDP,R_PGDP,R_DINC,R_UDINC,R_RDINC,R_LINV,R_HLINV,R_HSALES,R_SALES,P_THN,P_POP,P_GDP,P_PGDP,P_DINC," \
          "P_UDINC,P_RDINC,P_LINV,P_HLINV,P_HSALES,P_SALES,DIST from %s" % table2
    cur.execute(sql)
    results = cur.fetchall()
    columns = ['YEAR','POP','WS','HAMT','PPRICE','PRICE','THN','GDP','PGDP','DINC','UDINC','RDINC',
               'LINV','HLINV','HSALES','SALES','R_TNH','R_POP','R_GDP','R_PGDP','R_DINC','R_UDINC','R_RDINC','R_LINV',
               'R_HLINV','R_HSALES','R_SALES','P_TNH','P_POP','P_GDP','P_PGDP','P_DINC','P_UDINC','P_RDINC','P_LINV',
               'P_HLINV','P_HSALES','P_SALES','DIST']
    df = pd.DataFrame(list(results),
                      columns=columns)
    conn.close()
    return df


def mainWorkPrice(df,year):
    dic = {
        'YEAR':[year],
        'DIST':['诸暨'],
    }
    dic['WS'] = [arma_model(df,'WS')]
    for index in ['POP','HAMT','PPRICE','PRICE','THN','GDP','PGDP','DINC','UDINC','RDINC','LINV','HLINV','HSALES','SALES']:
        dic[index] = [lstm_model(df[index])]
    dic['PRICE'] = [-128122.5+1241.771* dic['POP'][0] -23.70197* dic['WS'][0]+0.003235*dic['HAMT'][0]]
    data_price = list(df['PRICE'])
    data_price.append(dic['PRICE'][0])
    for index_p,index_r,df_index in zip(
            ['P_THN','P_POP','P_GDP','P_PGDP','P_DINC','P_UDINC','P_RDINC','P_LINV','P_HLINV','P_HSALES','P_SALES'],
            ['R_THN','R_POP','R_GDP','R_PGDP','R_DINC','R_UDINC','R_RDINC','R_LINV','R_HLINV','R_HSALES','R_SALES'],
            ['THN','POP','GDP','PGDP','DINC','UDINC','RDINC','LINV','HLINV','HSALES','SALES']):
        data_index = list(df[df_index])
        data_index.append(dic['THN'][0])
        r,p = stats.pearsonr(data_price, data_index)
        dic[index_r], dic[index_p] = [r],[p]
    return dic


def getYear():
    '''
    获取明年的年份
    :return: year的int类型
    '''
    year = time.strftime('%Y',time.localtime(time.time()))
    return int(year)+1


def mianWorkRate(df,dist,year):
    '''
    计算模块
    :param df: 该区域的dataframe数据
    :param dist: 区分区域
    :param year: 当前年份
    :return: 预测完的dict形式
    '''
    dic = {}
    print('*'*30+dist+'***'+str(year)+'*'*30)
    s = lstm_model(df['s'])
    if dist == '暨阳街道':
        dic['pop'] = -156.098606076 + 0.0857575757655 * year
        dic['gdp'] = -13342.9042436 + 6.66830303091 * year
        dic['inc'] = 0.0
        dic['d'] = -13207.4988569 + 850.062765094 * dic['pop'] - 9.78650381334 * dic['gdp']
        dic['dist'] = '暨阳街道'
        dic['comp'] = 0.0
        dic['amt'] = 0.0
        dic['price'] = 0.0
    elif dist == '陶朱街道':
        dic['pop'] = -139.170848498 + 0.072060606067 * year
        dic['gdp'] = 0.0
        dic['inc'] = -3977024.05556 + 1987.95*year
        dic['d'] = 24.2271145745 - 2.99241039105 * dic['pop'] + 0.00136950651246 * dic['inc']
        dic['dist'] = '陶朱街道'
        dic['comp'] = 0.0
        dic['amt'] = 0.0
        dic['price'] = 0.0
    elif dist == '浣东街道':
        dic['pop'] = -114.780363647 + 0.0594545454599 * year
        dic['gdp'] = 0.0
        dic['inc'] = -47160752.2273 + 24909.4339821 * year -1861851.15651 * math.log(dic['pop'])
        dic['d'] = 0.00450561747435 * dic['inc'] - 13.3518737884 * dic['pop']
        dic['dist'] = '浣东街道'
        dic['comp'] = 0.0
        dic['amt'] = 0.0
        dic['price'] = 0.0
    elif dist == '店口镇':
        dic['pop'] = 0.0
        dic['gdp'] = -20518.7900625 + 10.2495757585 * year
        dic['inc'] = -11281616.6265 + 5677.88448182 * year - 23285.2522487 * math.log(dic['gdp'])
        dic['d'] = 10.0769619493 - 4.6953160152 * (math.sin(dic['inc']+dic['gdp'])) ** 3
        dic['dist'] = '店口镇'
        dic['comp'] = 0.0
        dic['amt'] = 0.0
        dic['price'] = 0.0
    elif dist == '枫桥镇':
        df_pop = df[df['pop']>0]['pop']
        dic['pop'] = lstm_model(df_pop)
        dic['gdp'] = -7771.90654616 + 3.88818181853 * year
        dic['inc'] = -3570291.30941 + 1784.6363638 * year
        dic['d'] = -726.333755735 + 100.759804602 * dic['pop'] - 0.449293110069 * dic['gdp'] + 0.00128071235614 * dic['inc']
        dic['dist'] = '枫桥镇'
        dic['comp'] = 0.0
        dic['amt'] = 0.0
        dic['price'] = 0.0
    elif dist == "诸暨":
        dic['dist'] = "诸暨"
        dic['pop'] = lstm_model(df['pop'])
        dic['gdp'] = 0.0
        dic['inc'] = 0.0
        dic['comp'] = lstm_model(df['comp'])
        dic['amt'] = lstm_model(df['amt'])
        dic['price'] = lstm_model(df['price'])
        dic['d'] = 86.88064 +  0.000164 * dic['amt'] - 0.015353 * dic['price']
        s = 634.3756 - 5.975481 * dic['pop'] + 0.685626 * dic['comp']
    dic['s'] = s
    dic['rate'] = s / dic['d']
    dic['year'] = year
    return dic


def mainRate():
    '''
    主程序
    :return: 无
    '''
    year = getYear()
    # year = 2019
    # 读取数据
    df = readTable1()
    all_dic = {'year':[],'s':[],'pop':[],'gdp':[],'inc':[],'d':[],'rate':[],'dist':[],'comp':[],'amt':[],'price':[]}
    # 判断是否预测过当前年份的数据,选择操作
    if len(df[df['year'] >= year]) == 0:
        for dist in ['暨阳街道', '陶朱街道', '浣东街道', '枫桥镇', '店口镇','诸暨']:
            df_dist = df[df['dist']==dist]
            dic = mianWorkRate(df_dist,dist,year)
            for key in all_dic:
                all_dic[key].append(dic[key])

    else:
        conn = pymysql.connect(host=host, port=port, user=user, password=password, db=db, charset='utf8')
        cur = conn.cursor()
        sql = "DELETE FROM %s WHERE year >= '%d'" % (table1,year)
        cur.execute(sql)
        conn.commit()
        conn.close()
        for dist in ['暨阳街道', '陶朱街道', '浣东街道', '枫桥镇', '店口镇','诸暨']:
            df_dist = df[df['dist'] == dist]
            df_dist = df_dist[df_dist['year']<year]
            dic = mianWorkRate(df_dist, dist,year)
            for key in all_dic:
                all_dic[key].append(dic[key])
    frame = pd.DataFrame(all_dic)
    commitDfToMysql(frame,table1)


def mainPrice():
    '''
    房价预测主程序
    :return: 无
    '''
    year_next = getYear()
    # year_next = 2018
    df = readTable2()
    year_max = df['YEAR'].max()
    if year_max < year_next:
        for year in range(year_max+1,year_next+1):
            dic = mainWorkPrice(df,year)
            frame = pd.DataFrame(dic)
            commitDfToMysql(frame,table2)
            df = readTable2()
    else:
        conn = pymysql.connect(host=host, port=port, user=user, password=password, db=db, charset='utf8')
        cur = conn.cursor()
        sql = "DELETE FROM %s WHERE year >= '%d'" % (table2,year_next)
        cur.execute(sql)
        conn.commit()
        conn.close()
        df = df[df['YEAR'] < year_next]
        dic = mainWorkPrice(df, year_next)
        frame = pd.DataFrame(dic)
        commitDfToMysql(frame, table2)


def main():
    '''
    主函数
    :return: 无
    '''
    start_time = time.time()
    mainRate()
    mainPrice()
    date = time.strftime('%H:%M:%S',time.localtime(time.time()-start_time))
    print('本次预测完成, 用时',date)


if __name__ == '__main__':
    main()