#!/usr/bin/python3
# -*- coding: utf-8 -*-


from __future__ import print_function
from scipy import stats
import pandas as pd
import statsmodels.api as sm


def arma_model(df,index):
    '''
    arma模型预测
    :param df: 所有数据的dataframe形式
    :param index: 列索引
    :return: 预测结果
    '''
    data = pd.DataFrame(df[index]).astype('float64')
    year = df['YEAR']
    year_max,year_min = year.max(),year.min()
    data.index = pd.Index(sm.tsa.datetools.dates_from_range(str(year_min), str(year_max)))
    arma_mod20 = sm.tsa.ARMA(data, (1, 0)).fit(disp=False)
    # print(arma_mod20.params)  # 参数
    sm.stats.durbin_watson(arma_mod20.resid.values)
    resid = arma_mod20.resid
    stats.normaltest(resid)
    r, q, p = sm.tsa.acf(resid.values.squeeze(), qstat=True)
    # data2 = np.c_[range(1, 11), r[1:], q, p]
    # table = pd.DataFrame(data2, columns=['lag', "AC", "Q", "Prob(>Q)"])
    # print(table.set_index('lag'))
    predict_sunspots = arma_mod20.predict(str(year_max+1), str(year_max+2), dynamic=True)
    # print(predict_sunspots)
    # print(predict_sunspots[0])
    return predict_sunspots[0]

if __name__ == '__main__':

    df = pd.read_excel("price.xlsx",sheet_name="all")
    arma_model(df,'PRICE')