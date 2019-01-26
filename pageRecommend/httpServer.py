#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
from flask import Flask,jsonify
from sklearn.externals import joblib

from kmeansResult import trainModel,recommendPage,kmeans,result

app = Flask(__name__)




@app.route('/train/', methods=['GET'])
def trainKmeansModel():
    """训练模型,将变量赋值给g"""
    trainModel()
    global kmeans
    global df
    global result
    kmeans = joblib.load("user_kmeans_model.m")
    df = pd.read_csv('trainResult.csv')
    result = df.groupby(['label', 'OBJECT_ID']).size()
    return "Completion of training"



@app.route('/recommend/<int:user_id>/',methods=['GET'])
def get_article(user_id):
    '''
    传入userId
    :param user_id: userId
    :return: 页面点击次数的倒序
    '''
    dic = recommendPage(user_id,kmeans,result)
    data = dict(dic)

    return jsonify(data)

# @app.errorhandler(404)
# def not_found(error):
#     return make_response(jsonify({'error': 'Not found'}), 404)


if __name__ == '__main__':
    # 启动服务前将全局变量读取好

    app.run(host='127.0.0.1', port=5633, debug=True)
