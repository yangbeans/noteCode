# -*- coding: utf-8 -*-
"""
Created on Wed Apr 28 14:21:15 2021

@author: yangsenbin

    通用的工具或工具类

"""
from pyspark.sql import SparkSession
from pyspark import SparkContext, StorageLevel, SparkConf
from pyspark.sql.types import *

#pandas_df、spark_df、hdfs读写与转换工具类
class WriteReadPandas2Hdfs:
    """
    功能：
        1 创建spark指挥官
        2 把pandas df写入到hdfs中；
        3 从hdfs读取文件变成spark df;
        4 pandas df转换为spark df；
    """
    
    #创建一个指挥官
    def create_sparkConf(self):
        conf = SparkConf().setAppName('test_text').setMaster('local[4]')
        conf.set('spark.driver.memory','4g') \
            .set('spark.executor.instances', '2') \
            .set('spark.executor.memory', '4g') \
            .set('spark.executor.cores', '3') \
            .set('spark.dynamicAllocation.enabled','false') \
            .set('spark.io.compression.codec','snappy') \
            .set('spark.sql.parquet.binaryAsString', 'true') 
        conf.set('spark.sql.parquet.binaryAsString', 'true')   ###***进制配置，避免读取parquet数据格式时出现乱码
        conf.set('spark.sql.csv.binaryAsString', 'true')
    
        sc = SparkContext('local', 'test', conf=conf)
        spark = SparkSession(sc)
        return spark
    
    #3 用pyspark写入到指定的建好的表中   把生成的taxi od保存到hdfs中  注：在此之前要建好存储的表格
    def write_pandasDF2hdfs(self, pandas_df, save_dir, partition_col, write_mode="append"):
        """
        parameters:
            pandas_df：待写入的pandasDF数据
            save_dir： 要保存到的hdfs文件地址
            partition_col： 存入后以该字段分区
            write_mode：写入模式，"append"-->在原有的基础上添加,"overwrite"-->覆盖
        """
        
        try:
            #创建一个指挥官
        #     spark = create_sparkConf()
            #把pandas dataframe转化为spark dataframe
            spark = self.create_sparkConf()
            values = pandas_df.values.tolist()
            
            columns = pandas_df.columns.tolist()
            #创建sparkDataFrame
            spark_df = spark.createDataFrame(values, columns)
            
            #把 spark df 写入指定的表中
            spark_df.write.partitionBy(partition_col).mode(write_mode).parquet(save_dir)
            spark.stop()
            print("写入完毕！")
        except Exception as e:
            spark.stop()
            
    #从hdfs读取数据变为sparkDF
    def read_hdfs2sparkDF(self, read_dir, file_type='parquet', index=False, header=True):
        spark_df = None
        try:
            spark = self.create_sparkConf()
            read_dir = "hdfs://10.91.125.8:8020/user/yangsenbin/bus_arrival_predict_time/model/rlmodel.pmml"
            spark_df = spark.read.format(file_type).load(read_dir, index=index, header=header)
            spark.stop()
            return spark_df
        except Exception as e:
            spark.stop()
            return spark_df
    
    #把pandasDF转换为sparkDF
    def transform_pandasDF2sparkDF(self, pandas_df):
        spark_df = None
        try:
            spark = self.create_sparkConf()
            values = pandas_df.values.tolist()
            columns = pandas_df.columns.tolist()
            spark_df = spark.createDataFrame(values, columns)
        except Exception as e:
            spark.stop()
            return spark_df
    
#----------------------------------------------------------------
#三种坐标系之间的相互转换

import numpy as np
import math
from math import cos, sin, pi
from decimal import *

class transfer:
    def __init__(self,key=None):
        self.a=6378245.0
        self.ee=Decimal(0.00669342162296594323)

    def transformLng(self,x,y):
        ret=Decimal()
        ret = 300.0+x+2.0*y+0.1*x*x+0.1*x*y+0.1*math.sqrt(math.fabs(x))
        ret += (20.0 * math.sin(6.0 * x * math.pi) + 20.0 * math.sin(2.0 * x * math.pi)) * 2.0 / 3.0
        ret += (20.0 * math.sin(x * math.pi) + 40.0 * math.sin(x / 3.0 * math.pi)) * 2.0 / 3.0
        ret += (150.0 * math.sin(x / 12.0 * math.pi) + 300.0 * math.sin(x / 30.0* math.pi)) * 2.0 / 3.0
        return ret

    def transformLat(self,x,y):
        ret = Decimal()
        ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y+ 0.2 * math.sqrt(math.fabs(x))
        ret += (20.0 * math.sin(6.0 * x * math.pi) + 20.0 * math.sin(2.0 * x * math.pi)) * 2.0 / 3.0
        ret += (20.0 * math.sin(y * math.pi) + 40.0 * math.sin(y / 3.0 * math.pi)) * 2.0 / 3.0
        ret += (160.0 * math.sin(y / 12.0 * math.pi) + 320 * math.sin(y * math.pi / 30.0)) * 2.0 / 3.0
        return ret

    def transfrom(self,lng,lat):
        dLat = self.transformLat(lng - 105.0, lat - 35.0)
        dLng = self.transformLng(lng - 105.0, lat - 35.0)
        radLat = lat / 180.0 * math.pi
        magic = math.sin(radLat)
        magic = 1 - self.ee * Decimal(magic) * Decimal(magic)
        sqrtMagic = math.sqrt(magic)
        dLat = Decimal((dLat * 180.0)) / ((Decimal(self.a) * (1 - self.ee)) / (Decimal(magic) * Decimal(sqrtMagic)) * Decimal(math.pi))
        dLng = (dLng * 180.0) / (self.a / sqrtMagic * math.cos(radLat) * math.pi)
        mgLat = lat + float(dLat)
        mgLng = lng + dLng
        return mgLng,mgLat

    #gps坐标转换为gcj02坐标系
    def wg84_to_gcj02(self,wg84_lng,wg84_lat):
        dLat=self.transformLat(wg84_lng-105.0,wg84_lat-35.0)
        dLng=self.transformLng(wg84_lng-105.0,wg84_lat-35.0)
        radLat = wg84_lat / 180.0 * math.pi
        magic = math.sin(radLat)
        magic = 1 - self.ee * Decimal(magic) * Decimal(magic)
        sqrtMagic = math.sqrt(magic)
        dLat = Decimal((dLat * 180.0)) / ((Decimal(self.a) * (1 - self.ee)) / (Decimal(magic) * Decimal(sqrtMagic)) * Decimal(math.pi))
        dLng = (dLng * 180.0) / (self.a / sqrtMagic * math.cos(radLat) * math.pi)
        gcj02Lat = wg84_lat + float(dLat)
        gcj02Lng = wg84_lng + dLng
        return gcj02Lng,gcj02Lat

    #gcj02坐标转百度坐标
    def gcj02_to_bd09(self,gcj02_lng,gcj02_lat):
        x = gcj02_lng
        y = gcj02_lat
        z = math.sqrt(x * x + y * y) + 0.00002 * math.sin(y * math.pi)
        theta = math.atan2(y, x) + 0.000003 * math.cos(x * math.pi)
        bd09_Lng = z * math.cos(theta) + 0.0065
        bd09_Lat = z * math.sin(theta) + 0.006
        return bd09_Lng,bd09_Lat

    #wg84坐标转百度坐标
    def wg84_to_bd09(self,wg84_lng,wg84_lat):
        gcj02lng,gcj02lat=self.wg84_to_gcj02(wg84_lng,wg84_lat)
        return self.gcj02_to_bd09(gcj02lng,gcj02lat)

    #百度坐标转GCJ02坐标
    def bd09_to_gcj02(self,bd09_lng,bd09_lat):
        x = bd09_lng - 0.0065
        y = bd09_lat - 0.006
        z = math.sqrt(x * x + y * y) - 0.00002 * math.sin(y * math.pi)
        theta = math.atan2(y, x) - 0.000003 * math.cos(x * math.pi)
        gcj02_lng = z * math.cos(theta)
        gcj02_lat = z * math.sin(theta)
        return gcj02_lng,gcj02_lat

    #GCJ坐标转WG84坐标
    def gcj02_to_wg84(self,gcj02_lng,gcj02_lat):
        mlng,mlat=self.transfrom(gcj02_lng,gcj02_lat)
        wg84_Lng=gcj02_lng*2-mlng
        wg84_Lat=gcj02_lat*2-mlat
        return wg84_Lng,wg84_Lat

    #将百度坐标转WG84坐标
    def bd09_to_wg84(self,bd09_lng,bd09_lat):
        gcj02_lng, gcj02_lat=self.bd09_to_gcj02(bd09_lng,bd09_lat)
        return self.gcj02_to_wg84(gcj02_lng,gcj02_lat)
    
tr=transfer()
#测试 
a, b = tr.bd09_to_gcj02(113.30764968,23.1200491)

print(tr.wg84_to_bd09(113.199559,23.162940)) 

print(tr.bd09_to_gcj02(113.21363095669257, 23.147216969649243))  #转换正确

print(tr.wg84_to_gcj02(113.500875,23.081212)) 

print(tr.gcj02_to_wg84(113.302992,23.385425)) 
        