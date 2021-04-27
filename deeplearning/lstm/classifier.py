# -*- coding: utf-8 -*-
"""
Created on Mon Apr 26 17:25:29 2021

@author: yangsenbin

    tensorflow 2.x版本
"""

from keras.models import Sequential
from keras.layers import *
from general import *
import os

#Seque构建方式（推荐）
class SequeClassifier():
    def __init__(self, units):
        self.units = units
        self.model = None
     
    #构建神经网络模型：（根据各层输入输出的shape）搭建网络结构、确定损失函数、确定优化器
    def build_model(self, loss, optimizer, metrics):
        self.model = Sequential()
        self.model.add(LSTM(self.units, return_sequences=True))
        self.model.add(LSTM(self.units))
        self.model.add(Dense(3, activation='softmax')) 
                         #最后一层全连接层。对于N分类问题，最后一层全连接输出个数为N个；对于回归问题，最后一层全连接层的输出为1
                        #激活函数也很重要，如果没有使用激活函数或者激活函数选择不当，很有可能产生梯度消失或梯度爆炸模型无法学习
        
        self.model.compile(loss=loss, 
                           optimizer=optimizer,   #优化器的选择很重要
                           metrics=metrics)
        

if __name__ == "__main__":
    #1 获取训练数据集，并调整为三维输入格式
    x_train, y_train, x_test, y_test = generate_classification_train_data()
    # 二维-->三维。构建的方法：
        # 方法一、直接 x_train[:, :, np.newaxis] 把原本二维数组中每一行变成二维，改变后每条记录shape变化为：(1,m)-->(m,1)
        # 方法二、对于时序问题，把每行记录拓展为包含包括该条记录时刻共n个时刻的记录，改变后每条记录shape变化为：(1,m)-->(n,m)
        # 方法三、对于原本二维数组中每一行，将其特征均等归类，归为k类，成为一个新的二维数组，改变后每条记录shape变化为：(1,m)-->(k,m/k)
        
    x_train = x_train[:, :, np.newaxis]
    x_test = x_test[:, :, np.newaxis]
    
    #2 构建神经网络模型：（根据各层输入输出的shape）搭建网络结构、确定损失函数、确定优化器
    units = 128 #lstm细胞个数
    loss = "sparse_categorical_crossentropy"  #损失函数类型
    optimizer = "adam"  #优化器类型
    metrics = ['accuracy']  #评估方法类型
    sclstm = SequeClassifier(units) 
    sclstm.build_model(loss, optimizer, metrics)

    #3 训练模型
    epochs = 30
    batch_size = 64
    sclstm.model.fit(x_train, y_train, epochs=epochs, batch_size=batch_size)

    #4 模型评估
    score = sclstm.model.evaluate(x_test, y_test, batch_size=16)
    print("model score:", score)
    
    # 模型应用：预测
    #proba_prediction = sclstm.model.predict(x_test)
    
    #5 模型持久化，把模型保存在本地
    dirs = "model"
    if not os.path.exists(dirs):
        os.makedirs(dirs)
    print("正在保存模型......")
    sclstm.model.save(dirs+"/classifier_model.h5")
    print("模型已保存.save path-->dirs%s"%"/classifier_model.h5")
    
    #6 从指定模型保存的位置读取模型，做预测
    from keras.models import load_model
    read_model = load_model(dirs+"/classifier_model.h5")
    out = read_model.predict(x_test)
    print("out:%s"%out)
    
    
