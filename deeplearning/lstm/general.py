# -*- coding: utf-8 -*-
"""
Created on Mon Apr 26 17:50:32 2021

@author: yangsenbin
"""

import numpy as np
import pandas as pd
import tensorflow as tf

from tensorflow import keras
from tensorflow.keras import layers

from sklearn import datasets
from sklearn.cross_validation import train_test_split



#生成回归训练集
def generate_regression_train_data():
    dataset_path = keras.utils.get_file("auto-mpg.data", "http://archive.ics.uci.edu/ml/machine-learning-databases/auto-mpg/auto-mpg.data")
    dataset_path
    column_names = ['MPG','Cylinders','Displacement','Horsepower','Weight',
                'Acceleration', 'Model Year', 'Origin']
    raw_dataset = pd.read_csv(dataset_path, names=column_names,
                          na_values = "?", comment='\t',
                          sep=" ", skipinitialspace=True)
    dataset = raw_dataset.copy()
    dataset = dataset.dropna()
    origin = dataset.pop('Origin')
    dataset['USA'] = (origin == 1)*1.0
    dataset['Europe'] = (origin == 2)*1.0
    dataset['Japan'] = (origin == 3)*1.0
    
    train_dataset = dataset.sample(frac=0.8,random_state=0)
    test_dataset = dataset.drop(train_dataset.index)
    train_stats = train_dataset.describe()
    train_stats.pop("MPG")
    train_stats = train_stats.transpose()
    y_train = train_dataset.pop('MPG')
    y_test = test_dataset.pop('MPG')
    def norm(x):
        return (x - train_stats['mean']) / train_stats['std']
    
    X_train = norm(train_dataset)
    X_test = norm(test_dataset)
    x_train = np.array(X_train)
    x_test = np.array(X_test)
    y_train = np.array(y_train)
    y_test = np.array(y_test)
    return x_train, y_train, x_test, y_test

#X_train, y_train, X_test, y_test = generate_regression_train_data()
    

#生成分类数据集
def generate_classification_train_data():
    lris_df = datasets.load_iris()
    X_data = lris_df.data
    y_data = lris_df.target
    X_train,X_test,y_train,y_test=train_test_split(X_data,y_data,test_size=0.2)

    x_train = np.array(X_train)
    x_test = np.array(X_test)
    y_train = np.array(y_train)
    y_test = np.array(y_test)
    return x_train, y_train, x_test, y_test




#x_train, y_train, x_test, y_test = generate_classification_train_data()
