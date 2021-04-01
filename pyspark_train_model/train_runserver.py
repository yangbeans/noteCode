# -*- coding: utf-8 -*-
"""
Created on Thu Feb  4 17:40:05 2021

@author: yangsenbin
"""

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext, StorageLevel, SparkConf
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType, StringType
import numpy as np
import math
from math import cos, sin, pi
import os
import math
from decimal import *
import time
from time import strftime, localtime

from pyspark.sql.functions import col, lit, from_json,to_timestamp, unix_timestamp, hour, minute

import pyspark.ml.feature as mlft
import pyspark.ml.regression as rgr
#from mmlspark import LightGBMRegressor
from pyspark.ml import Pipeline
#from pyspark2pmml import PMMLBuilder
import sys
import pyspark2pmml
from general import _logging
from general import get_model
from generate_knowledge_trainData import GenerateKnowledge, GenerateTrainData

class TrainModel:
    def __init__(self, train_data_dir, save_model_dir):
        #self.bus_ad_data_dir = bus_ad_data_dir
        self.train_data_dir = train_data_dir
        self.save_model_dir = save_model_dir
        #os.makedirs("./logs/log", exist_ok=True)
        #self.plogger_ = _logging(filename='./logs/log/trainlog'+str(os.getpid())+'.log')  #预测的日志
        
    def create_sparkSession(self, master):
        conf = SparkConf().setAppName('test_text').setMaster(master) #.set("spark.jars", "jars/jpmml-sparkml-executable-1.4-SNAPSHOT.jar")
        #conf = SparkConf().setAppName('test_text')
        conf.set("spark.io.compression.codec", "snappy") \
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .set('spark.sql.parquet.binaryAsString', 'true') \
            .set('spark.sql.csv.binaryAsString', 'true') \
            .set('spark.driver.memory','4g') \
            .set('spark.executor.instances', '3') \
            .set('spark.executor.cores', '3') \
            .set('spark.dynamicAllocation.enabled','false')
    
        ###***进制配置，避免读取parquet数据格式时出现乱码
        #pyFiles=["D:/project_code/src_bigdata_machine_learning/src/bus_arrival_time_predict/codePysaprk/train/pyts.zip"]
        sc = SparkContext(master=master, conf=conf)  #="yarn-cluster"
        #sc = SparkContext(master, 'test', conf=conf)
        try:
            spark = SparkSession(sc)  
            return sc, spark
        except Exception as e:
            print("create sc error:%s"%e)
            sc.stop()  #防止报错时该sparkSecession还遗留
            return None, None
    
    def generate_traindata(self, spark, train_start_date, train_end_date):
        #* 生成训练集，保存到hdfs中，并返回文件保存的hdfs地址
        # 生成历史特征群表
        #gk = GenerateKnowledge()
        #gk.generate_knowledge(train_start_date, train_end_date)
        
        # 生成训练集
        #gt = GenerateTrainData()
        #gt.generate_all_train_data(train_start_date, train_end_date)
        #train_data_dir = 'hdfs://10.91.125.8:8020/user/hive/warehouse/algorithm_db.db/batp_train_data' 
        print("get train data...")
        train_data = spark.read.format('parquet').load(self.train_data_dir, index=False, header=True) #.filter((col("pdate")>=train_start_date) & (col("pdate")<=train_end_date))
        print("get train data done!")
        #train_data.createOrReplaceTempView("test")\
    
    #     train_data = spark.read.option("delimiter", "\001").option("header", "false").format('csv').load(train_data_dir)  #, index=False, header=True
    
    #     #把spark df注册成表
    #     #train_data.createOrReplaceTempView("partitions_od_bus_station")
        #* 把字段变为模型可接收的数据类型
        # 要填充缺失值，不然有缺失值时会报错
        print("format field...")
        train_data = train_data.fillna(-1)
        train_data = train_data.withColumn('conbine_id', train_data['conbine_id'].cast('double'))\
           .withColumn('mid_num', train_data['mid_num'].cast('double'))\
           .withColumn('direction', train_data['direction'].cast('double'))\
           .withColumn('minute5_parti', train_data['minute5_parti'].cast('double'))\
           .withColumn('minute10_parti', train_data['minute10_parti'].cast('double'))\
           .withColumn('minute15_parti', train_data['minute15_parti'].cast('double'))\
           .withColumn('minute30_parti', train_data['minute30_parti'].cast('double'))\
           .withColumn('hour_parti', train_data['hour_parti'].cast('double'))\
           .withColumn('week_parti', train_data['week_parti'].cast('double'))\
           .withColumn('holiday', train_data['holiday'].cast('integer'))\
           .withColumn('last1_waitingtime', train_data['last1_waitingtime'].cast('double'))\
           .withColumn('last2_waitingtime', train_data['last2_waitingtime'].cast('double'))\
           .withColumn('last3_waitingtime', train_data['last3_waitingtime'].cast('double'))\
           .withColumn('last4_waitingtime', train_data['last4_waitingtime'].cast('double'))\
           .withColumn('last5_waitingtime', train_data['last5_waitingtime'].cast('double'))\
           .withColumn('avg_last', train_data['avg_last'].cast('double'))\
           .withColumn('diff_12', train_data['diff_12'].cast('double'))\
           .withColumn('diff_23', train_data['diff_23'].cast('double'))\
           .withColumn('diff_34', train_data['diff_34'].cast('double'))\
           .withColumn('diff_45', train_data['diff_45'].cast('double'))\
           .withColumn('slop_12', train_data['slop_12'].cast('double'))\
           .withColumn('slop_23', train_data['slop_23'].cast('double'))\
           .withColumn('slop_34', train_data['slop_34'].cast('double'))\
           .withColumn('slop_45', train_data['slop_45'].cast('double'))\
           .withColumn('conbine_avg', train_data['conbine_avg'].cast('double'))\
           .withColumn('conbine_max', train_data['conbine_max'].cast('double'))\
           .withColumn('conbine_min', train_data['conbine_min'].cast('double'))\
           .withColumn('week_avg', train_data['week_avg'].cast('double'))\
           .withColumn('week_max', train_data['week_max'].cast('double'))\
           .withColumn('week_min', train_data['week_min'].cast('double'))\
           .withColumn('week_triplog_num', train_data['week_triplog_num'].cast('double'))\
           .withColumn('hour_avg', train_data['hour_avg'].cast('double'))\
           .withColumn('hour_max', train_data['hour_max'].cast('double'))\
           .withColumn('hour_min', train_data['hour_min'].cast('double'))\
           .withColumn('weekhour_avg', train_data['weekhour_avg'].cast('double'))\
           .withColumn('weekhour_max', train_data['weekhour_max'].cast('double'))\
           .withColumn('weekhour_min', train_data['weekhour_min'].cast('double'))\
           .withColumn('holiday_avg', train_data['holiday_avg'].cast('double'))\
           .withColumn('holiday_max', train_data['holiday_max'].cast('double'))\
           .withColumn('holiday_min', train_data['holiday_min'].cast('double'))\
           .withColumn('holiday_triplog_num', train_data['holiday_triplog_num'].cast('double'))\
           .withColumn('is_peak_hour', train_data['is_peak_hour'].cast('double'))\
           .withColumn('label', train_data['label'].cast('double'))\
           .withColumn('route_id', train_data['route_id'].cast('double'))
        print("format field done!")
        return train_data
                   
    def train_model(self, train_start_date, train_end_date, master='local[*]'):
        #self.plogger_.info("创建sparkSession中......")
        print("sparkSession building......")
        sc, spark = self.create_sparkSession(master)
        print("sparkSession built！")
        try:  #防止报错时该sparkSecession还遗留
            
            print("train data is generating......")
            train_data = self.generate_traindata(spark, train_start_date, train_end_date)
            print("train data generated！")
            #print("train data:\n", train_data)
            print("training model......")
            #heattraindata,heattestdata = train_data1.randomSplit([0.7,0.3],seed=666)
            select_feature = train_data.columns
            featuresCreator = mlft.VectorAssembler(
                inputCols=[col for col in select_feature[:-2]]+["route_id"],  ###***选择特征的字段名称，注意，不要把标签字段名称也加进去
                outputCol='features')
            # fomula = RFormula(formula = 'label ~ .')
            rf = rgr.RandomForestRegressor(
    #             maxIter=100,
                #regParam=0.01,
                labelCol='label')
    
            pipeline = Pipeline(stages=[featuresCreator,rf])
            # pipeline = Pipeline(stages=[fomula,logistic])
            model = pipeline.fit(train_data)
            print("trained model!")
            
            #pmmlBuilder = PMMLBuilder(sc, train_data, model)
            #pmmlBuilder.buildFile(dirs+"/lrmodel.pmml")
            #pmmlBuilder.buildFile("hdfs://10.91.125.8:8020/user/hive/warehouse/rlmodel.pmml")
            #modelPmml = pmmlBuilder.build()
            
            
            #modelString = PMMLBuilder(sc, train_data, model).buildByteArray().decode() #模型生成pmml格式并转为string
            #print("modelString:\n", modelString)
    #         print("model string:\n", modelString)
            
            #保存模型到hdfs中
            modelString = get_model()
            model_sparkDF = spark.createDataFrame([[modelString]],["model"])
            version_id = strftime("%Y-%m-%d %H:%M:%S", localtime()).replace("-","").replace(" ","").replace(":","")
            model_addr = self.save_model_dir+"/rfmodel"+version_id+".pmml"
            model_sparkDF.write.mode('overwrite').parquet(model_addr)
            print("modelString:", modelString)
            print("模型训练完毕并保存在：%s"%model_addr)
            print("model id trained!")
            
            sc.stop()
            #return modelString
        except Exception as e:
            print("main running error:%s"%e)
            sc.stop()
            #return None

train_data_dir = 'hdfs://10.91.125.8:8020/user/hive/warehouse/algorithm_db.db/batp_train_data'  #训练集保存的位置
save_model_dir = "hdfs://10.91.125.8:8020/user/yangsenbin/bus_arrival_predict_time/model"  # 模型训练完后保存的位置
master_yarn = "yarn-cluster"
master_local = "local[*]"
tm = TrainModel(train_data_dir, save_model_dir)
tm.train_model("2021-02-01", "2021-03-03", master=master_yarn)


