# -*- coding: utf-8 -*-
"""
Created on Tue Apr 27 09:48:39 2021

@author: yangsenbin
"""
import  tensorflow as tf
from tensorflow import keras
from keras.models import Sequential
from keras.layers import *
from general import *
import  tensorflow as tf




class SequeRegressor():
    def __init__(self, units):
        self.units = units
        self.model = None
     
    #构建神经网络模型：（根据各层输入输出的shape）搭建网络结构、确定损失函数、确定优化器
    def build_model(self, loss, optimizer, metrics):
        self.model = Sequential()
        self.model.add(LSTM(self.units, return_sequences=True))
        self.model.add(LSTM(self.units))
        self.model.add(Dense(1))
        
        self.model.compile(loss=loss, 
              optimizer=optimizer,   #优化器的选择很重要
              metrics=metrics)
        

if __name__ == "__main__":
    #1 获取训练数据集，并调整为三维输入格式
    x_train, y_train, x_test, y_test = generate_regression_train_data()
    x_train = x_train[:, :, np.newaxis]
    x_test = x_test[:, :, np.newaxis]
    
    #2 构建神经网络模型：（根据各层输入输出的shape）搭建网络结构、确定损失函数、确定优化器
    units = 64 #lstm细胞个数
    loss = "mse"  #损失函数类型
    optimizer = tf.keras.optimizers.RMSprop(0.001)  #优化器类型
    metrics = ['mae', 'mse']  #评估方法类型
    srlstm = SequeRegressor(units) 
    srlstm.build_model(loss, optimizer, metrics)
    
    #3 训练模型
    epochs = 35
    batch_size = 32
    srlstm.model.fit(x_train, y_train, epochs=epochs, batch_size=batch_size)
    
    #4模型评估
    score = srlstm.model.evaluate(x_test, y_test, batch_size=16)
    print("model score:", score)
    
    #5 模型应用：预测
    proba_prediction = srlstm.model.predict(x_test)
        