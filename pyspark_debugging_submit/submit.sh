spark2-submit --master yarn --deploy-mode cluster --conf spark.sql.execution.arrow.enabled=true --conf spark.pyspark.driver.python=/opt/cloudera/parcels/Anaconda-5.1.0.1/bin/python3 --conf spark.pyspark.python=/opt/cloudera/parcels/Anaconda-5.1.0.1/bin/python3 --jars hdfs://nameservice1/user/yangsenbin/project/bus_predict_endurance_mileage/jars --py-files='hdfs://nameservice1/user/yangsenbin/project/bus_predict_endurance_mileage/pyts.zip' hdfs://nameservice1/user/yangsenbin/project/bus_predict_endurance_mileage/main.py