#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
# from sklearn.preprocessing import MinMaxScaler
import pandas as pd

# lstm 预测模型

def lstm_model(datafram_index):
    '''
    模型预测
    :param datafram_index: dataframe 需要预测的列
    :return: 预测结果
    '''
    dataset = datafram_index.values.astype('float32')
    max_value = dataset.max()
    min_value = dataset.min()
    arr = []
    for i in dataset:
        arr.append([[(i-min_value)/max_value]])
    dataset = numpy.array(arr)
    dataX = dataset[:-1]
    dataY = dataset[1:]
    # 随机种子
    numpy.random.seed(7)
    # scaler = MinMaxScaler(feature_range=(0, 1))
    look_back = 1
    trainX = dataX
    trainY = numpy.array([i[0][0] for i in dataY])
    trainX = numpy.reshape(trainX, (trainX.shape[0], 1, trainX.shape[1]))
    predictData = numpy.reshape(dataset, (dataset.shape[0], 1, dataset.shape[1]))

    model = Sequential()
    model.add(LSTM(32, input_shape=(1, look_back)))
    model.add(Dense(1))
    model.compile(loss='mean_squared_error', optimizer='adam')
    model.fit(trainX, trainY, epochs=200, batch_size=1, verbose=2)
    trainPredict = model.predict(predictData)
    # print('***预测结果***')
    result = trainPredict[-1][0]*max_value+min_value
    # for i in trainPredict:
    #     print(str(i[0]*max_value+min_value))
    return result


if __name__ == '__main__':
    df = pd.read_excel("/home/xjh/桌面/a.xlsx",sheet_name='h')
    result = lstm_model(df['SALES'])
    print(result)