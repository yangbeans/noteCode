目的：该代码示例是实现pyspark处理海量数据（包括建模）的结构框架。包括本地电脑调试pyspark、yarn集群提交的实现

操作步骤：
    <1> 把pyts.zip、jars文件夹、main.py文件放在指定的文件目录下（linux文件系统或hdfs文件系统都可以）；
    <2> 提交pyspark代码并运行
        本地调试模式：本地电脑安装pyspark，然后直接在本地电脑 main.py调试即可
        yarn集群模式：
            在submit.sh文件中修改spark.pyspark.driver.python、spark.pyspark.python和pyts.zip、jars文件夹、main.py文件为自己实际的环境和路径，
            然后启动submit.sh文件即可（bash -e submit.sh，或者用Hue平台workflow执行，或者用调度器（如海豚调度器）执行）。

注意：
    1 submit.sh 提交命令shell代码中，不要换行，间隔中间用一个空格隔开
    
    2 pyspark主函数py文件放在最后一个位置（如例中的main.py）
    
    3 文件中的路径可以是linux的路径或者hdfs路径，都必须是绝对路径
        其中，如果选择hdfs路径的话，格式为 hdfs://nameservice（或spark namenode节点所在服务器ip）/绝对路径，如： hdfs://nameservice1/user/yangsenbin/project/bus_predict_endurance_mileage/main.py
        本例大数据平台的spark namenode节点服务器ip映射的名称是nameservice1
        运行代码相关文件存放位置说明：
            pyspark主程序代码文件
                把main.py文件放在大数据平台的 /user/yangsenbin/project/bus_predict_endurance_mileage/ 目录下；
            运行主程序所需的jar文件
                把运行主程序所需的jar包统一放在文件夹jars里，该文件夹放在大数据平台的 /user/yangsenbin/project/bus_predict_endurance_mileage/ 目录下；
            运行主程序所需的相关py依赖模块（即.py文件）放在pyts文件夹中，这里一般是业务逻辑处理部分的项目结构代码（详见pyts文件夹结构），并通过zip打包成pyts.zip。 该pyts.zip放在大数据平台的 /user/yangsenbin/project/bus_predict_endurance_mileage/ 目录下
                其中，打包pyts.zip的过程：
                    <1> 在linux环境中，cd 进入到pyts文件夹中；
                    <2> 执行文件夹打包命令行： # zip -r pyts.zip ./*
                    <3> 打包好的typs.zip放到指定目录下
        注：在写shell脚本时spark-submit命令是可以自动识别hdfs文件目录的。如果是其他命令，如python命令，是不能识别hdfs命令，只能是linux机器的路径
        
    4 pyspark主函数py文件中要有启动sparksession的相关代码，不然提交程序会报错。比如，例中的main.py不能只是一行 print(1) 代码，详见main.py文件代码
        main.py函数注意内容：
            <1> 本地调试时 master='local[*]'  yarn集群运行时 master='yarn-cluster'
                读取文件的路径地址记得修改为相应环境下的地址
            <2> spark-submit提交运行时，统一打包主程序运行所需的py文件到pyts.zip文件夹中，并命令行指定路径（--py-files=pyts.zip的绝对路径）。
                在main.py文件中就可以直接import pyts.zip里的所有.py模块
            <3> 引入jar包的方式有两种：
                一、os.environ['PYSPARK_SUBMIT_ARGS'] = "--jars "+jar绝对路径+" pyspark-shell"
                二、在spark-submit提交运行时，命令行指定所需jar绝对路径或jar所在的文件夹绝对路径
            <4> 在conf参数设置时，要根据环境情况，对conf.set(...) 里的参数做调整和增加
            <5> 读取avro文件时需要引入依赖jar包，具体读取avrowen类型的文件见代码。（avro数据格式很难用，一般情况下，先将avro格式转换为parquet或其他容易使用的数据格式）
            <6> 从内存角度考虑，从hdfs读取整个文件后，应按一定的条件进行过滤，采用filter()函数
                例：data1 = spark.read.format('parquet').load("hdfs://10.91.125.8:8020/user/hive/warehouse/algorithm_db.db/can_dispatch", index=False, header=True) \
                            .filter((col("pdate")>='2021-07-02') & (col("pdate")<='2021-07-03')) 
            
    5 submit shell代码和pyspark主函数py文件具体参考例中的 submit.sh 和main.py

总结：
    一、上述注意所述；
    二、读取数据：
        难用的数据格式在平台上很难读取和应用，如avro，这时需要把该种格式转换为简单易用，性能较好的格式，如parquet，再进行其他逻辑处理；
        在部署的过程中，读parquet格式文件有时报错：java.lang.NoClassDefFoundError、有时正常。原因是集群某一个或某些机器缺少相应支持jar包导致
    三、sql处理数据：
        用该种方式读取数据然后注册成view，用sparksql模块sql语句处理数据的本质是spark集群直接与hdfs文件系统做交互实现计算，
        与用spark api本质一致。不涉及到与hive交互。
        正常运行的条件是：
            <1> spark集群各节点功能正常；
            <2> spark与hdfs文件系统连接正常，即spark集群与hdfs文件系统网络能通；
    四、spark-submit时，注意对spark的几个常见重要参数进行调参，调参的主要目的是在spark运行最小内存不溢出、以及合理分配资源的前提下让spark集群计算速度更快。