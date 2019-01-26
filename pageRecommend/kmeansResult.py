#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.externals import joblib
import os

from mergeTable3 import dataframe,user_df


def trainModel():
    '''
    将训练模型及数据 保存到本地
    '''
    # 去重
    df = dataframe
    df_user = df[~pd.DataFrame.duplicated(df, subset="USER_ID", keep='first')]
    df_user_ID = df_user[['USER_ID']]
    df_user = df_user[['SEX', 'ORG_ID', 'POSITION_ID', 'AGE', 'ARE1', 'ARE2', 'ARE3']]
    arr = np.array(df_user)

    kmeans = KMeans(n_clusters=10, random_state=0).fit(arr)
    label = kmeans.labels_
    # print(label)
    df_user_ID['label'] = label

    joblib.dump(kmeans, "user_kmeans_model.m")
    df = pd.merge(df,df_user_ID,on="USER_ID")
    df.to_csv("trainResult.csv",index=False)


def recommendPage(user_id,kmeans,result):
    '''
    传入user_id,kmeans模型,已经训练数据df
    :param user_id: user_id
    :param kmeans: 模型
    :param result: gorupby后的结果
    :return: 推荐页面 object_id 降序排列的list
    '''
    
    train = user_df[user_df['USER_ID'] == user_id][['SEX', 'ORG_ID', 'POSITION_ID', 'AGE', 'ARE1', 'ARE2', 'ARE3']][:1]
    train = np.array(train).tolist()
    label = kmeans.predict(train)[0]

    dic = list(result[label].items())
    dic.sort(reverse=True,key=lambda x:x[1])
    return dic



if os.path.exists("user_kmeans_model.m"):
    kmeans = joblib.load("user_kmeans_model.m")
    df = pd.read_csv('trainResult.csv')
    result = df.groupby(['label', 'OBJECT_ID']).size()
else:
    trainModel()
    kmeans = joblib.load("user_kmeans_model.m")
    df = pd.read_csv('trainResult.csv')
    result = df.groupby(['label', 'OBJECT_ID']).size()

# # 读取本地kmeans模型
# kmeans = joblib.load("user_kmeans_model.m")
# # 读取本地训练好的数据df
# df = pd.read_csv('trainResult.csv')
#
# result = df.groupby(['label', 'OBJECT_ID']).size()

