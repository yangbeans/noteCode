# -*- coding: utf-8 -*-


import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext, StorageLevel, SparkConf
import os

# 从pyts包中import
from pyts.etl import data_etl #
from pyts import utils
#from etl import data_etl

avro_jar_dir = "file:///D:/project_code/src_tmp/src_ai_bus_predict_endurance_mileage/src/V1.0/test/spark_submit/jars/spark-avro_2.11-4.0.0.jar"
#avro_jar_dir = "hdfs://nameservice1/user/yangsenbin/project/bus_predict_endurance_mileage/jars/spark-avro_2.11-4.0.0.jar"

master = 'local[*]'  # 本地电脑代码调试时，master选择这个模式
#master = 'yarn-cluster' # 提交yarn集群时，master要选择这个模式


# 创建SparkConf（sc）和SparkSession（spark）
def create_sparkSession(master):
    # 添加业务需求中pyspark运行所需的jar包
    # 添加读取avro所需的依赖jar包
    #os.environ['PYSPARK_SUBMIT_ARGS'] = "--jars "+avro_jar_dir+" pyspark-shell" #如果是linux路径（不是hdfs路径），这里路径开头要记得加 "file:///"
    
    # 创建SparkConf配置对象,并命名
    conf = SparkConf().setAppName('test_text')
    # 配置spark相关的参数
    conf.set("spark.io.compression.codec", "snappy") \
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .set('spark.sql.parquet.binaryAsString', 'true') \
        .set('spark.sql.csv.binaryAsString', 'true') \
        .set('spark.driver.memory','1g') \
        .set('spark.executor.instances', '2') \
        .set('spark.executor.cores', '2') \
        .set('spark.dynamicAllocation.enabled','false')

    ###***进制配置，避免读取parquet数据格式时出现乱码
    
    # 该py文件运行时所需的其他py文件
    
    # 创建SparkContext对象
    sc = SparkContext(master=master, conf=conf)  #="yarn-cluster"
    try:
        spark = SparkSession(sc)  
        return sc, spark
    except Exception as e:
        print("create sc error:%s"%e)
        sc.stop()  #防止报错时该sparkSecession还遗留
        return None, None



sc, spark = create_sparkSession(master)

try:
    # 创建SparkSession对象
    spark = SparkSession(sc)  
    print("reading data1......")
    # 读取can原始数据
    # 读取 parquet类型文件
    data1 = spark.read.format('parquet').load("hdfs://10.91.125.8:8020/user/hive/warehouse/algorithm_db.db/routes_index_result", index=False, header=True)
    #data1 = spark.read.format('parquet').load("hdfs://nameservice1/user/hive/warehouse/algorithm_db.db/routes_index_result", index=False, header=True)
    
    # 读取avro类型的文件，记得添加读取avro所需jar包，jar包有两种添加方式：<1> 如上，os.environ['PYSPARK_SUBMIT_ARGS']中加入；<2> spark-submit 命令行时指定jar包或jar包所在的文件夹路径
    # data1 = spark.read.format('com.databricks.spark.avro').load("hdfs://10.91.125.8:8020/user/hive/warehouse/apts_db.db/ods_bus_can_dispatch", index=False, header=True)
    
    print("read!")
    
    # 业务逻辑处理。这个过程一般要把sc,spark、data这三个参数放到业务逻辑处理主函数中
    print("Business logic processing......")
    data_etl(sc, spark, data1)
    print("processed!")
    
    # 把数据写到指定位置
    print("writing......")
    #data1.write.mode('append').partitionBy("route_id_run").parquet("hdfs://10.91.125.8:8020/user/hive/warehouse/algorithm_db.db/routes_index_result3")
    print("write!")
    sc.stop()
except Exception as e:
    sc.stop()
    
    