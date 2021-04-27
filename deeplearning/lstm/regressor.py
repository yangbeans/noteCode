# -*- coding: utf-8 -*-
"""
Created on Mon Apr 26 17:27:03 2021

@author: yangsenbin
    tensorflow 2.x版本
"""



import  os
import  tensorflow as tf
import  numpy as np
from tensorflow import keras
from general import *

#import  tensorflow as tf
#from tensorflow import keras
from keras.models import Sequential
from keras.layers import *
#from general import *


tf.random.set_seed(22)
np.random.seed(22)
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
assert tf.__version__.startswith('2.')

#Func构建方式
class FuncMRegressor(keras.Model):
    def __init__(self, units, num_classes, num_layers):
        super(FuncMRegressor, self).__init__()

        # self.cells = [keras.layers.LSTMCell(units) for _ in range(num_layers)]
        #
        # self.rnn = keras.layers.RNN(self.cells, unroll=True)
        self.rnn = keras.layers.LSTM(units, return_sequences=True)
                        # return_sequences设置lstm中单个细胞中返回时间点的类型。非最后一层，return_sequences=True才能满足三维输入的数据格式要求
                        #默认 False。在输出序列中，返回单个 hidden state值还是返回全部time step 的 hidden state值。 False 返回单个， true 返回全部。
                    
        self.rnn2 = keras.layers.LSTM(units) #最后一层，return_sequences=False，对于单个样本而言，返回的是一维（对于所有样本是二维）
        
#         self.fc1 = layers.Dense(64, activation='relu')
#         self.fc2 = layers.Dense(64, activation='relu')

        # self.cells = (keras.layers.LSTMCell(units) for _ in range(num_layers))
        # 
        # self.rnn = keras.layers.RNN(self.cells, return_sequences=True, return_state=True)
        # self.rnn = keras.layers.LSTM(units, unroll=True)
        # self.rnn = keras.layers.StackedRNNCells(self.cells)

        # have 1000 words totally, every word will be embedding into 100 length vector
        # the max sentence lenght is 80 words
#         self.embedding = keras.layers.Embedding(1000, 100, input_length=9)  
                            #指定隐含层的shape-->[top_words,100]，input_length参数是最初输入的二维矩阵的维度。
                            # 这样，根据隐含层原理，隐含层输出的数据shape为-->[None,input_length,100]=[None,80,100]
#         self.soft_max = keras.layers.Softmax()
#         self.fc1 = keras.layers.Dense(32, activation='relu')   

        self.fc3 = keras.layers.Dense(1)  #最后一层全连接层。对于N分类问题，最后一层全连接输出个数为N个；对于回归问题，最后一层全连接层的输出为1
   
    #定义输入输出
    def call(self, inputs, training=None, mask=None):  ###*** 该函数重写了RNN的call()方法，该方法是模型输出表达式（即，模型）
        #print('y', inputs.shape)
        #1 利用隐含层将二维数组变为三维数组
        #    [None,80]-->[None,80,100]
        
        # [b, sentence len] => [b, sentence len, word embedding]
#         y = self.embedding(inputs)  #用隐含层的目的是让本来二维的数据变成三维输入到lstm中（大多数的深度学习算法都要求输入的数据是三维，对于传统的如房价预测，需要将二维数组通过一定方法转换为3维数组）
#         print("y1 shape:",y.shape)
        #2 得到了满足lstm（大多数深度学习算法）输入的三维数据格式后，搭建lstm网络
        
        #搭建神经网络：可以根据需要搭建输入、多层结构层、dropout、激活函数、池化等
        y = self.rnn(inputs)   #要求输入到lstm模型中的数据必须是三维
        #print("y2 shape:", y.shape)  
        y = self.rnn2(y) 
        # print('rnn', x.shape)
        #3 确定输出。
        #y = self.fc1(y)   #全连接层
        #print(y.shape)
        #y = self.fc2(y)
        y = self.fc3(y)
        return y

#Seque构建方式（推荐）
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
    print("**********************【Func搭建方式】********************")
    # 获取二维数组（特征提取过的）
    x_train, y_train, x_test, y_test = generate_regression_train_data()
    # 把二维数组改为三维数组（通过np.newaxis随机增加一维数组）
    x_train = x_train[:, :, np.newaxis]
    x_test = x_test[:, :, np.newaxis]
    
    #训练模型
    units = 64   #细胞个数
    num_classes = 1  #回归问题，该参数为1
    batch_size = 32
    epochs = 35
    model = FuncMRegressor(units, num_classes, num_layers=2)
    optimizer = tf.keras.optimizers.RMSprop(0.001)
    
    model.compile(loss='mse',
                optimizer=optimizer,
                metrics=['mae', 'mse'])
    
    
    # model.compile(optimizer=keras.optimizers.Adam(0.001),
    #                   loss=keras.losses.BinaryCrossentropy(from_logits=False),
    #                   metrics=['accuracy', 'mse'])
    model.fit(x_train, y_train, batch_size=batch_size, epochs=epochs,
                  validation_data=(x_test, y_test), verbose=1)
    
    # 模型应用预测
    #model.predict只返回y_pred
    out = model.predict(x_train)
    #print("out:", out)
    #evaluate用于评估您训练的模型。它的输出是准确度或损失，而不是对输入数据的预测。
    scores = model.evaluate(x_test, y_test, batch_size, verbose=1)
    print("Final test loss and accuracy :", scores)
    
    
    print("**********************【Seque搭建方式】********************")
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
    prediction = srlstm.model.predict(x_test)
    
    
    