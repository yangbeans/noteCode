#!/bin/bash
#spark2-submit --master yarn --deploy-mode cluster --driver-memory 4g --conf spark.dynamicAllocation.enabled=false --conf spark.sql.execution.arrow.enabled=true --conf spark.pyspark.driver.python=/usr/local/anaconda3/envs/py3/bin/python --conf spark.pyspark.python=/usr/local/anaconda3/envs/py3/bin/python --jars /root/tmpFile/bus_arrival_predict_time/train/jars/jpmml-sparkml-executable-1.5-SNAPSHOT.jar --py-files /root/tmpFile/bus_arrival_predict_time/train/general.py /root/tmpFile/bus_arrival_predict_time/train/train_runserver.py

#spark2-submit --master local[*] --driver-memory 4g --conf spark.dynamicAllocation.enabled=false --conf spark.sql.execution.arrow.enabled=true --conf spark.pyspark.driver.python=/root/anaconda3/bin/python3 --conf spark.pyspark.python=/root/anaconda3/bin/python3 --jars /root/project/bus_arrival_time_predict/train/jars/jpmml-sparkml-executable-1.4-SNAPSHOT.jar --py-files /root/project/bus_arrival_time_predict/train/general.py /root/project/bus_arrival_time_predict/train/generate_knowledge_trainData.py /root/project/bus_arrival_time_predict/train/train_runserver.py


#spark2-submit --master yarn --deploy-mode cluster --conf spark.sql.execution.arrow.enabled=true --conf spark.pyspark.driver.python=/usr/local/anaconda3/bin/python3 --conf spark.pyspark.python=/usr/local/anaconda3/bin/python3 --jars /root/project/bus_arrival_time_predict/train/jars/jpmml-sparkml-executable-1.4-SNAPSHOT.jar /root/project/bus_arrival_time_predict/train/train_runserver.py --py-files='/root/project/bus_arrival_time_predict/train/pyts.zip'

#spark2-submit --master yarn --deploy-mode cluster --conf spark.sql.execution.arrow.enabled=true --conf spark.pyspark.driver.python=/usr/local/anaconda3/bin/python3 --conf spark.pyspark.python=/usr/local/anaconda3/bin/python3 --jars /root/project/bus_arrival_time_predict/train/jars/jpmml-sparkml-executable-1.4-SNAPSHOT.jar --py-files='/root/project/bus_arrival_time_predict/train/demoTTTT.zip' /root/project/bus_arrival_time_predict/train/demoMain.py



spark2-submit --master yarn \
              --deploy-mode cluster \
              --conf spark.sql.execution.arrow.enabled=true \
              --conf spark.pyspark.driver.python=/usr/local/anaconda3/bin/python3 \
              --conf spark.pyspark.python=/usr/local/anaconda3/bin/python3 \
              --jars /root/project/bus_arrival_time_predict/train/jars/jpmml-sparkml-executable-1.4-SNAPSHOT.jar \
              --py-files='/root/project/bus_arrival_time_predict/train/pyutil.zip' \
              /root/project/bus_arrival_time_predict/train/train_runserver.py