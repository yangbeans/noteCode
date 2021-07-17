# -*- coding: utf-8 -*-

# import sys
# sys.path.append("../")  # 要有
from utils.egg import *

def data_etl(sc, spark, data1):
    print("data is etling......")
    data1.createOrReplaceTempView("ods_bus_can_dispatch")
    data11 = spark.sql("select * from ods_bus_can_dispatch")
    print("data11.columns=", data11.columns)
    data_egg()
    print("etl!")
    
