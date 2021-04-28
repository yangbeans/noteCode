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
    
            
        