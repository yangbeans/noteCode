# -*- coding: utf-8 -*-

# pyspark与hive sql交互


import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext, StorageLevel, SparkConf
import os

import argparse
import configparser
import os

# 获取配置文件对象
def get_config(conf_file):
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default=conf_file, help="configuration file path", required=False)
    parser.add_argument("--force", type=str, default=True, help="remove params dir", required=False)
    
    try:
        args = parser.parse_args() #call from command line
    except:
        args = parser.parse_args(args=[]) #call from notebook
    
    # mxboard log dir
    if os.path.exists('logs'):
        shutil.rmtree('logs')
        print('Remove log dir')
    
    # read configuration
    config = configparser.ConfigParser()
    print('Read configuration file: %s' % (args.config))
    config.read(args.config)
    return config


# 获取配置文件对象，并对所需配置参数全局化
config = get_config("configurations/myconf.conf")
etl_config = config['Etl']
data_dir1 = etl_config["data_dir1"] 
result_dir = etl_config["result_dir"] # 结果表存储位置
master = etl_config["spark_master"]

class PysparkSql:
    #创建spark session
    def create_sparkSession(self, master):
        # 加入依赖，用于读取avro文件
        os.environ['PYSPARK_SUBMIT_ARGS'] = '''--jars file:///D:/project_code/src_tmp/src_ai_bus_predict_endurance_mileage/src/V1.0/jars/spark-avro_2.11-4.0.0.jar pyspark-shell'''
        
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
        
    # 用pyspark与hive sql交互，调用sql做数据处理（ETL）
    def etl4sql(self, spark):
        # 从hdfs读取原始数据
        #data_dir1 = 'hdfs://10.91.125.8:8020/user/hive/warehouse/algorithm_db.db/routes_index_result' 
        print("reading data1......")
        # avro 读取
        data1 = spark.read.format('parquet').load(data_dir1, index=False, header=True)
        #data1 = spark.read.format('com.databricks.spark.avro').load(data_dir1, index=False, header=True)
        print("read!")
        print("data1.columns=", data1.columns)
        datas = [data1]
        
        # ETL
        #result_dir = 'hdfs://10.91.125.8:8020/user/hive/warehouse/algorithm_db.db/tmp_etl_result' 
        
        etl_result = self.udf_etl(spark, datas, result_dir)
        
        # 其他操作
        # etl_result_df = etl_result.toPandas()
        
        
        return etl_result.head(10)
    
    # ETL处理核心函数，需要根据不同业务重写
    def udf_etl(self, spark, datas, result_dir):  
        data10 = datas[0]
        #1 注册视图，供sql语句做数据预处理
        data10.createOrReplaceTempView("view_data10")
        
        # ETL
        etl_sql1 = """select route_id_run, comp, ct from view_data10"""
        data11 = spark.sql(etl_sql1)
        print("data11.columns:", data11.columns)
        data11.createOrReplaceTempView("view_data11")
        
        etl_sql2 = """select comp, route_id_run from view_data11"""
        data12 = spark.sql(etl_sql2)
        print("data12.columns:", data12.columns)
        
        # 把文件写入hdfs指定位置
        print("writing data......")
        data12.write.mode('append').partitionBy("route_id_run").parquet(result_dir)
        #data12.write.mode('append').parquet(result_dir)
        print("write!")
        return data12
    
    def test_etl4sql(self, master='local[*]'):
        sc, spark = self.create_sparkSession(master)
        try:

            data1 = self.etl4sql(spark)
            sc.stop()
            return data1
        except Exception as e:
            print("e2:%s"%e)
            sc.stop()
            return None
        
if __name__ == "__main__":
    ps = PysparkSql()
    etl_result = ps.test_etl4sql()