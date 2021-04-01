# -*- coding: utf-8 -*-
"""
Created on Wed Mar  3 19:28:14 2021

@author: yangsenbin
"""

from impala.dbapi import connect
import pandas as pd

#获取最近有在运行的路线
def get_allids_sql(train_start_date, train_end_date):
    train_start_date2 = train_start_date.replace("-", "")
    train_end_date2 = train_end_date.replace("-", "")
    conn = connect(host='172.31.200.171',port=8017)#21000,query.noobank.com
    cur = conn.cursor()
    sql = """with tt0 as 
            (select cur_station_id, order_number, arrival_time, depart_time, triplog_id, route_id_run, direction, pdate
            from idatatech_db.partitions_od_bus_station
            where pdate between %s and %s  -- 【can be select】
            ),

            -- 提取正常营运的记录
            TRM AS
            (select triplog_id, max(run_mileage) run_mileage, max(run_duration) run_duration, max(employee_id) employee_id, max(service_name) service_name
            from apts_db.original_triplog_new
            where run_date between %s and %s
            group by triplog_id),"""%("'"+train_start_date+"'", "'"+train_end_date+"'", "'"+train_start_date2+"'", "'"+train_end_date2+"'")+\
            """normal_ad_data AS
            (select tt0.*, TRM.service_name
            from 
                tt0
            left join TRM
            on tt0.triplog_id = TRM.triplog_id),

            t0 AS
            (select * from normal_ad_data
            where service_name like '短线%'
            or service_name like '区间%'
            or service_name like '快车%'
            or service_name like '全程%')
            (select distinct route_id_run from t0)"""
    #print("sql1:", sql1)
    cur.execute(sql)
    cur.close
    result = cur.fetchall()
    allids = pd.DataFrame(data = list(result))
    allids.columns = ["route_id_run"]
    allids_list = list(allids["route_id_run"])
    return allids_list

#生成所有历史特征表
class GenerateKnowledge:
    # 生成所有线路的历史特征表
    def generate_knowledge(self, train_start_date, train_end_date):
        #1 删除旧历史特征表并创建新表
        self.create_table_knowledge()
        
        #2 得到所有线路的id
        allids_list = get_allids_sql(train_start_date, train_end_date)
        allids_list = ["6250", "55"]
        #3 依次求得所有线路的历史特征表并插入结果表中
        route_num = len(allids_list)
        for i in range(route_num):
#             if i % 10 == 0:
            print("正在获取第%s条线路（%s）的历史特征群，共%s条线路......"%(i+1, allids_list[i], route_num))
            route_idi = allids_list[i]
            self.generate_routeidi_knowledge(route_idi, train_start_date, train_end_date)
    
    #生成某条线路所有历史特征表，并插入到指定的表中  操作这一步之前要先判断是否存在重名的之前生成的表，如果是，要删除
    def generate_routeidi_knowledge(self, route_id, train_start_date, train_end_date):
        conn = connect(host='172.31.200.171',port=8017)
        cur = conn.cursor()
        
        #生成所有历史特征表
        #删除上条线路留下的临时表，并创建新的临时表
        #print("删除上一条路线产生的旧临时表，新base data 临时表落磁盘")
        self.get_general_base_sql(cur, route_id, train_start_date, train_end_date)
        
        sql_hf_week = self.get_hf_week_sql()
        sql_hf_hour = self.get_hf_hour_sql()
        sql_hf_weekhour = self.get_hf_weekhour_sql()
        sql_hf_holiday = self.get_hf_holiday_sql()
        sql_hf_conbine = self.get_hf_conbine_sql()
        
        #print("sql_hf_week")
        cur.execute(sql_hf_week)  #在这里只能一条sql 执行一次
        #print("sql_hf_hour")
        cur.execute(sql_hf_hour)
        #print("sql_hf_weekhour")
        cur.execute(sql_hf_weekhour)
        #print("sql_hf_holiday")
        cur.execute(sql_hf_holiday)
        #print("sql_hf_conbine")
        cur.execute(sql_hf_conbine)
        cur.close
        #print("done!")
    
    #创建历史特征群表
    def create_table_knowledge(self):
        conn = connect(host='172.31.200.171',port=8017)
        cur = conn.cursor()

        #删除旧表
        sql_drop_conbine = """drop table if exists algorithm_db.batp_knowledge_conbine"""   
        sql_drop_holiday = """drop table if exists algorithm_db.batp_knowledge_holiday"""
        sql_drop_hour = """drop table if exists algorithm_db.batp_knowledge_hour"""
        sql_drop_week = """drop table if exists algorithm_db.batp_knowledge_week"""
        sql_drop_weekhour = """drop table if exists algorithm_db.batp_knowledge_weekhour"""

        # 建表
        sql_create_t_conbine = """CREATE TABLE algorithm_db.batp_knowledge_conbine (  
            route_id_run STRING,   conbine_id STRING,   mid_num BIGINT,   conbine_avg DOUBLE,  
            conbine_max BIGINT,   conbine_min BIGINT,   conbine_peak_hours STRING ) 
            STORED AS parquet;"""

        sql_create_t_holiday = """CREATE TABLE algorithm_db.batp_knowledge_holiday (   
            route_id_run STRING,   conbine_id STRING,   mid_num BIGINT,   holiday TINYINT,
            holiday_avg DOUBLE,   holiday_max BIGINT,   holiday_min BIGINT,   holiday_triplog_num DOUBLE ) 
            STORED AS parquet;"""

        sql_create_t_hour = """CREATE TABLE algorithm_db.batp_knowledge_hour (   
            route_id_run STRING,   conbine_id STRING,   mid_num BIGINT,   hour_parti INT,   
            hour_avg DOUBLE,   hour_max BIGINT,   hour_min BIGINT ) 
            STORED AS parquet;"""

        sql_create_t_week = """CREATE TABLE algorithm_db.batp_knowledge_week (  
            route_id_run STRING,   conbine_id STRING,   mid_num BIGINT,   week_parti BIGINT,  
            week_avg DOUBLE,   week_max BIGINT,   week_min BIGINT,   week_triplog_num DOUBLE ) 
            STORED AS parquet;"""

        sql_create_t_weekhour = """CREATE TABLE algorithm_db.batp_knowledge_weekhour (   
            route_id_run STRING,   conbine_id STRING,   mid_num BIGINT,   week_parti BIGINT,  
            hour_parti INT,   weekhour_avg DOUBLE,   weekhour_max BIGINT,   weekhour_min BIGINT ) 
            STORED AS parquet;"""

        print("正在删除旧历史特征群表")
        cur.execute(sql_drop_conbine)  #在这里只能一条sql 执行一次
        cur.execute(sql_drop_holiday)
        cur.execute(sql_drop_hour)
        cur.execute(sql_drop_week)
        cur.execute(sql_drop_weekhour)
        print("历史特征群旧表删除完毕！")
        print("建新的特征群表")
        cur.execute(sql_create_t_conbine)  #在这里只能一条sql 执行一次
        cur.execute(sql_create_t_holiday)
        cur.execute(sql_create_t_hour)
        cur.execute(sql_create_t_week)
        cur.execute(sql_create_t_weekhour)
        print("特征群表创建完成！")
        cur.close

    #基础数据通用查询语句，生成临时表落磁盘
    def get_general_base_sql(self, cur, route_id, train_start_date, train_end_date):
        #删除上条线路留下的临时表,并创建新的临时表
        sql_drop_tmp_old = """drop table if exists algorithm_db.batp_tmp_kbase_data"""
        
        train_start_date2 = train_start_date.replace("-", "")
        train_end_date2 = train_end_date.replace("-", "")
        
        sql_base_data = """create table algorithm_db.batp_tmp_kbase_data as
                with tt0 as 
                (select cur_station_id, order_number, arrival_time, depart_time, triplog_id, route_id_run, direction, pdate
                from idatatech_db.partitions_od_bus_station
                where pdate between %s and %s  -- 【can be select】
                and route_id_run = %s  -- 【can be select, if train all of GZ' lines, it must be annotated】
                ),

                -- 提取正常营运的记录
                TRM AS
                (select triplog_id, max(run_mileage) run_mileage, max(run_duration) run_duration, max(employee_id) employee_id, max(service_name) service_name
                from apts_db.original_triplog_new
                where run_date between %s and %s
                group by triplog_id),"""%("'"+train_start_date+"'", "'"+train_end_date+"'", "'"+route_id+"'", "'"+train_start_date2+"'", "'"+train_end_date2+"'")+\
                """normal_ad_data AS
                (select tt0.*, TRM.service_name
                from 
                    tt0
                left join TRM
                on tt0.triplog_id = TRM.triplog_id),

                t0 AS
                (select * from normal_ad_data
                where service_name like '短线%'
                or service_name like '区间%'
                or service_name like '快车%'
                or service_name like '全程%'),
                -- (select * from t0)

                -- 【Fillna time】
                t1 as
                (select *
                from 
                    (select cur_station_id, order_number, triplog_id, route_id_run, direction, pdate, 
                                case
                                    when ((arrival_time = '') and (depart_time != ''))  then  from_unixtime(unix_timestamp(depart_time) - 40)   -- (depart_time - INTERVAL 40 SECOND) milliseconds_add(depart_time, -40*1000)  
                                    when (arrival_time != '') then arrival_time
                                    when ((arrival_time = '') and (depart_time = '')) then ''
                                    end arrival_time,    
                                case 
                                    when (depart_time = '' and arrival_time != '') then from_unixtime(unix_timestamp(depart_time) + 40) --milliseconds_add(arrival_time, 40*1000)  
                                    when (depart_time != '') then depart_time
                                    when (depart_time = '' and arrival_time = '') then ''
                                    end depart_time
                    from t0) tmp0
                where depart_time is not null and arrival_time is not null
                and arrival_time < depart_time),  -- Remove outliers if arrival_time <= depart_time
                -- (select * from t1 limit 100)

                -- 【Generate base data】
                BASE AS
                (select distinct *  --去重
                from 
                    (select t1.cur_station_id start_id,t1.order_number start_order_number,t1.depart_time start_time, t1.triplog_id, t1.route_id_run, t1.direction, t1.pdate,
                            t2.cur_station_id end_id, t2.arrival_time end_time, t2.order_number end_order_number
                    from t1 join t1 t2
                    on t1.triplog_id = t2.triplog_id
                    where cast(t1.order_number as int) < cast(t2.order_number as int)) base1),
                -- (select * from BASE limit 100)

                -- 【Data preprocessing】 
                -- 【Remove outliers】
                -- 【If start_time > end_time, remove them; or if diff time bigger then we can accept】
                -- 【part egg】
                R as  
                (select *
                from 
                    (select *, concat(start_id, end_id) conbine_id,
                                (cast(end_order_number as int)-cast(start_order_number as int)) mid_num,
                                hour(start_time) hour_parti,
                                cast((hour(start_time)*60+minute(start_time))/5 as int) minute5_parti,
                                cast((hour(start_time)*60+minute(start_time))/10 as int) minute10_parti,
                                cast((hour(start_time)*60+minute(start_time))/15 as int) minute15_parti,
                                cast((hour(start_time)*60+minute(start_time))/30 as int) minute30_parti,
                                (dayofweek(start_time)-1) week_parti,
                                -- 【is weekend】
                                case 
                                    when (dayofweek(start_time)-1 = 6) or (dayofweek(start_time)-1 = 7) then 1 
                                    else 0 
                                    end holiday,
                                (unix_timestamp(end_time)-unix_timestamp(start_time)) label
                    from BASE
                    where start_time < end_time) clearn0_data
                where label <= 60*60*4   -- 【remove when label > 4 hours】
                and label >= 10)
                (select * from R)"""
        cur.execute(sql_drop_tmp_old)
        cur.execute(sql_base_data)
    
    # 生成行驶区间在不同工作日类型的表现情况表
    def get_hf_week_sql(self):
        sql = """-- 生成历史特征群的基础数据库
                -- 各个行驶区间在不同时间刻度的表现
                -- 各个行驶区间在不同工作日类型的表现
                with R as (select * from algorithm_db.batp_tmp_kbase_data),
                HF_WEEK AS
                (select 
                    route_id_run,conbine_id,mid_num,week_parti,
                    avg(label) week_avg, max(label) week_max,min(label) week_min,
                    ndv(triplog_id)/ndv(pdate) week_triplog_num  -- 不同工作日类型平均一天的发车次数
                from 
                    R
                GROUP BY 
                    route_id_run,conbine_id,mid_num,week_parti)
                -- (select * from HF_WEEK)
                insert into algorithm_db.batp_knowledge_week select * from HF_WEEK"""
        return sql
    
    def get_hf_hour_sql(self):
        sql = """-- 生成历史特征群的基础数据库
                -- 各个行驶区间在不同时间刻度的表现
                -- 各个行驶区间在不同小时段的表现
                with R as (select * from algorithm_db.batp_tmp_kbase_data),
                HF_HOUR AS
                (select 
                    route_id_run,conbine_id,mid_num,hour_parti,
                    avg(label) hour_avg, max(label) hour_max,min(label) hour_min
                from 
                    R
                GROUP BY 
                    route_id_run,conbine_id,mid_num,hour_parti)
                -- (select * from HF_HOUR)
                insert into algorithm_db.batp_knowledge_hour select * from HF_HOUR"""
        return sql 
    
    def get_hf_weekhour_sql(self):
        sql = """-- 生成历史特征群的基础数据库
                -- 各个行驶区间在不同时间刻度的表现
                -- 各个行驶区间在不同工作日类型的不同小时段的表现
                with R as (select * from algorithm_db.batp_tmp_kbase_data),
                HF_WEEK_HOUR AS
                (select 
                    route_id_run,conbine_id,mid_num,week_parti, hour_parti,
                    avg(label) weekhour_avg, max(label) weekhour_max,min(label) weekhour_min
                from 
                    R
                GROUP BY 
                    route_id_run,conbine_id,mid_num,week_parti, hour_parti)
                -- (select * from HF_WEEK_HOUR)
                insert into algorithm_db.batp_knowledge_weekhour select * from HF_WEEK_HOUR"""
        return sql

    def get_hf_holiday_sql(self):
        sql = """-- 生成历史特征群的基础数据库
                -- 各个行驶区间在不同时间刻度的表现
                -- 各个行驶区间在不同工作日类型的不同小时段的表现
                -- 各个行驶区间在节假日与工作日的表现
                with R as (select * from algorithm_db.batp_tmp_kbase_data),
                HF_HOLIDAY AS
                (select 
                    route_id_run,conbine_id,mid_num,holiday,
                    avg(label) holiday_avg, max(label) holiday_max,min(label) holiday_min,
                    ndv(triplog_id)/ndv(pdate) holiday_triplog_num
                from 
                    R
                GROUP BY 
                    route_id_run,conbine_id,mid_num,holiday)
                -- (select * from HF_HOLIDAY)
                insert into algorithm_db.batp_knowledge_holiday select * from HF_HOLIDAY"""
        return sql
    
    def get_hf_conbine_sql(self):
        sql = """-- 生成历史特征群的基础数据库
                -- 各个行驶区间在不同时间刻度的表现
                -- 各个行驶区间在不同工作日类型的不同小时段的表现
                -- 不同行驶区间的历史表现情况
                -- 不同区间的平均行驶时长
                with R as (select * from algorithm_db.batp_tmp_kbase_data),
                HF_HOUR AS
                (select 
                    route_id_run,conbine_id,mid_num,hour_parti,
                    avg(label) hour_avg, max(label) hour_max,min(label) hour_min
                from 
                    R
                GROUP BY 
                    route_id_run,conbine_id,mid_num,hour_parti),

                conbine_tmp0 AS
                (select 
                    route_id_run,conbine_id,mid_num,
                    avg(label) conbine_avg,  max(label) conbine_max,min(label) conbine_min
                from 
                    R
                group by 
                    route_id_run,conbine_id,mid_num),
                -- (select * from conbine_tmp0)

                -- 不同行驶区间的高峰小时段
                -- 合并每个行驶区间的平均行驶时长
                conbine_tmp1 as
                (select ct0.*, conbine_tmp0.conbine_avg
                from 
                    (select *,
                        row_number() over (partition by route_id_run,conbine_id,mid_num order by hour_avg desc) rank_hour_dt
                    from 
                        HF_HOUR) ct0
                left join
                    conbine_tmp0
                on ((ct0.route_id_run=conbine_tmp0.route_id_run) and (ct0.conbine_id=conbine_tmp0.conbine_id) and (ct0.mid_num=conbine_tmp0.mid_num))),
                -- (select * from conbine_tmp1 order by route_id_run,conbine_id,mid_num,rank_hour_dt)

                -- 得到每个行驶区间一天中的高峰小时段
                conbine_tmp2 as
                (select 
                    route_id_run,conbine_id,mid_num,
                    group_concat(cast(hour_parti as string), ',') conbine_peak_hours
                from 
                    conbine_tmp1
                where 
                    hour_avg > conbine_avg
                    and rank_hour_dt <= 4 
                group by
                    route_id_run,conbine_id,mid_num),
                -- (select * from conbine_tmp2)

                -- 合并行驶区间表现特征
                HF_CONBINE AS
                (select 
                    conbine_tmp0.*, conbine_tmp2.conbine_peak_hours
                from
                    conbine_tmp0
                left join
                    conbine_tmp2
                on ((conbine_tmp0.route_id_run=conbine_tmp2.route_id_run) and (conbine_tmp0.conbine_id=conbine_tmp2.conbine_id) and (conbine_tmp0.mid_num=conbine_tmp2.mid_num)))
                -- (SELECT * FROM HF_CONBINE)
                insert into algorithm_db.batp_knowledge_conbine select * from HF_CONBINE"""
        return sql
    
class GenerateTrainData:
    #所有线路的训练集生成
    def generate_all_train_data(self, train_start_date, train_end_date):
        #1 删除上次训练时留下的旧的train_data，并创建新的train_data空表
        self.create_table_train_data()

        #2 得到所有线路的id
        allids_list = get_allids_sql(train_start_date, train_end_date)

        #3 依次求得所有线路的历史特征表并插入结果表中
        allids_list = ["6250", "55"]
        route_num = len(allids_list)
        for i in range(route_num):
            print("正在获取第%s条线路（%s）的训练集，共%s条线路......"%(i+1, allids_list[i], route_num))
            route_idi = allids_list[i]
            self.get_routeidi_train_data(route_idi, train_start_date, train_end_date)

    def create_table_train_data(self):
        conn = connect(host='172.31.200.171',port=8017)
        cur = conn.cursor()
        sql_drop_train_data = """drop table if exists algorithm_db.batp_train_data"""
        sql_create_new_train_data = """CREATE TABLE algorithm_db.batp_train_data (  
                conbine_id STRING,   mid_num BIGINT,   direction STRING, 
                minute5_parti INT,   minute10_parti INT,   minute15_parti INT,   minute30_parti INT, 
                hour_parti INT,   week_parti BIGINT,   holiday TINYINT,   last1_waitingtime BIGINT,   
                last2_waitingtime BIGINT,   last3_waitingtime BIGINT,   last4_waitingtime BIGINT,   last5_waitingtime BIGINT,   
                avg_last DOUBLE,   diff_12 BIGINT,   diff_23 BIGINT,   diff_34 BIGINT,   
                diff_45 BIGINT,   slop_12 DOUBLE,   slop_23 DOUBLE,   slop_34 DOUBLE,  
                slop_45 DOUBLE,   conbine_avg DOUBLE,   conbine_max BIGINT,   conbine_min BIGINT,  
                week_avg DOUBLE,   week_max BIGINT,   week_min BIGINT,   week_triplog_num DOUBLE, 
                hour_avg DOUBLE,   hour_max BIGINT,   hour_min BIGINT,   weekhour_avg DOUBLE, 
                weekhour_max BIGINT,   weekhour_min BIGINT,   holiday_avg DOUBLE,   holiday_max BIGINT, 
                holiday_min BIGINT,   holiday_triplog_num DOUBLE,   is_peak_hour TINYINT,   label BIGINT ) 
                PARTITIONED BY (   route_id STRING )
                STORED AS parquet """
        cur.execute(sql_drop_train_data)
        cur.execute(sql_create_new_train_data)
        cur.close

    # 某一条线路的训练数据集生成
    def get_routeidi_train_data(self,route_idi, train_start_date, train_end_date):
        conn = connect(host='172.31.200.171',port=8017)
        cur = conn.cursor()
        #1 删除上一条路线存在的临时表,并生成新的临时表并落磁盘
        self.get_tmp_tbase_data(cur, route_idi, train_start_date, train_end_date)

        #2 生成该线路训练集数据，并插入到指定的结果表中
        self.insert_into_result_table(cur, route_idi)

        #3 关闭sql
        cur.close

    def get_tmp_tbase_data(self, cur, route_idi, train_start_date, train_end_date):
        train_start_date2 = train_start_date.replace("-", "")
        train_end_date2 = train_end_date.replace("-", "")

        sql_drop_old_tmp = """drop table if exists algorithm_db.batp_tmp_tbase_data"""
        sql_tbase_data = """-- 生成临时表并落磁盘
                create table algorithm_db.batp_tmp_tbase_data as
                with tt0 as 
                (select cur_station_id, order_number, arrival_time, depart_time, triplog_id, route_id_run, direction, pdate
                from idatatech_db.partitions_od_bus_station
                where pdate between %s and %s  -- 【can be select】
                and route_id_run in (%s)  -- 【can be select, if train all of GZ' lines, it must be annotated】
                ),

                -- 提取正常营运的记录
                TRM AS
                (select triplog_id, max(run_mileage) run_mileage, max(run_duration) run_duration, max(employee_id) employee_id, max(service_name) service_name
                from apts_db.original_triplog_new
                where run_date between %s and %s
                group by triplog_id),"""%("'"+train_start_date+"'", "'"+train_end_date+"'", "'"+route_idi+"'", "'"+train_start_date2+"'", "'"+train_end_date2+"'")+\
                """normal_ad_data AS
                (select tt0.*, TRM.service_name
                from 
                    tt0
                left join TRM
                on tt0.triplog_id = TRM.triplog_id),

                t0 AS
                (select * from normal_ad_data
                where service_name like '短线%'
                or service_name like '区间%'
                or service_name like '快车%'
                or service_name like '全程%'),
                -- (select * from t0)

                -- 【Fillna time】
                t1 as
                (select *
                from 
                    (select cur_station_id, order_number, triplog_id, route_id_run, direction, pdate, 
                                case
                                    when ((arrival_time = '') and (depart_time != ''))  then  from_unixtime(unix_timestamp(depart_time) - 40)   -- (depart_time - INTERVAL 40 SECOND) milliseconds_add(depart_time, -40*1000)  
                                    when (arrival_time != '') then arrival_time
                                    when ((arrival_time = '') and (depart_time = '')) then ''
                                    end arrival_time,    
                                case 
                                    when (depart_time = '' and arrival_time != '') then from_unixtime(unix_timestamp(depart_time) + 40) --milliseconds_add(arrival_time, 40*1000)  
                                    when (depart_time != '') then depart_time
                                    when (depart_time = '' and arrival_time = '') then ''
                                    end depart_time
                    from t0) tmp0
                where depart_time is not null and arrival_time is not null
                and arrival_time < depart_time),  -- Remove outliers if arrival_time <= depart_time
                -- (select * from t1 limit 100)

                -- 【Generate base data】
                BASE AS
                (select distinct *  --去重
                from 
                    (select t1.cur_station_id start_id,t1.order_number start_order_number,t1.depart_time start_time, t1.triplog_id, t1.route_id_run, t1.direction, t1.pdate,
                            t2.cur_station_id end_id, t2.arrival_time end_time, t2.order_number end_order_number
                    from t1 join t1 t2
                    on t1.triplog_id = t2.triplog_id
                    where cast(t1.order_number as int) < cast(t2.order_number as int)) base1),
                -- (select * from BASE limit 100)

                -- 【Data preprocessing】 
                -- 【Remove outliers】
                -- 【If start_time > end_time, remove them; or if diff time bigger then we can accept】
                -- 【part egg】
                clearn_partEgg_data as  
                (select *
                from 
                    (select *, concat(start_id, end_id) conbine_id,
                                (cast(end_order_number as int)-cast(start_order_number as int)) mid_num,
                                hour(start_time) hour_parti,
                                cast((hour(start_time)*60+minute(start_time))/5 as int) minute5_parti,
                                cast((hour(start_time)*60+minute(start_time))/10 as int) minute10_parti,
                                cast((hour(start_time)*60+minute(start_time))/15 as int) minute15_parti,
                                cast((hour(start_time)*60+minute(start_time))/30 as int) minute30_parti,
                                (dayofweek(start_time)-1) week_parti,
                                -- 【is weekend】
                                case 
                                    when (dayofweek(start_time)-1 = 6) or (dayofweek(start_time)-1 = 7) then 1 
                                    else 0 
                                    end holiday,
                                (unix_timestamp(end_time)-unix_timestamp(start_time)) label
                    from BASE
                    where start_time < end_time) clearn0_data
                where label <= 60*60*4   -- 【remove when label > 4 hours】
                and label >= 10),
                -- (select * from clearn_partEgg_data limit 100)

                -- *日期重构

                -- 【egg:last one drivingtime in the same line】
                last_mergerfilter_data as 
                (select clearn_partEgg_data.*, cpd2.start_time lasti_start_time, cpd2.end_time lasti_end_time
                from clearn_partEgg_data
                join clearn_partEgg_data cpd2
                on ((clearn_partEgg_data.route_id_run=cpd2.route_id_run) and (clearn_partEgg_data.conbine_id = cpd2.conbine_id)) -- 【join clearn_partEgg_data self limited when both route_id_run and conbine_id issame】
                where clearn_partEgg_data.start_time > cpd2.start_time  -- 【filter1: cpd2 data is later than clearn_partEgg_data】
                and clearn_partEgg_data.start_time > cpd2.end_time  -- 【filter2: when this bus is start from start_id, lasti bus was arrival to the end_id】
                --and clearn_partEgg_data.pdate between days_add(cpd2.pdate, -1) and cpd2.pdate
                -- and clearn_partEgg_data.pdate between (cpd2.pdate - INTERVAL 1 DAY) and cpd2.pdate   from_unixtime(unix_timestamp(depart_time) - 40)
                and clearn_partEgg_data.pdate between substr(from_unixtime(unix_timestamp(cpd2.pdate) - 86400), 1, 10) and cpd2.pdate 
                ),
                -- (select * from last_mergerfilter_data limit 100)

                R AS
                (select distinct *  --去重
                from 
                    (select *,
                            row_number() over (partition by route_id_run, conbine_id, start_time order by lasti_start_time desc) rank_number --route_id_run, conbine_id, start_id, start_order_number, start_time, triplog_id, direction, pdate, end_id, end_order_number, end_time, conbine_id, mid_num, 
                    from last_mergerfilter_data) rr)
                (select * from R)"""
        cur.execute(sql_drop_old_tmp)
        cur.execute(sql_tbase_data)


    def insert_into_result_table(self, cur,route_idi):
        sql = """-- 这一步之前，历史特征群已生成

                -- 各个行驶区间在不同时间刻度的表现
                -- 各个行驶区间在不同工作日类型的表现
                with R AS (SELECT * FROM algorithm_db.batp_tmp_tbase_data),
                HF_WEEK AS
                (select * from algorithm_db.bapt_knowledge_week where route_id_run=%s)  ,
                -- (select * from HF_WEEK)

                -- 各个行驶区间在不同小时段的表现
                HF_HOUR AS
                (select 
                    *
                from 
                    algorithm_db.bapt_knowledge_hour
                where route_id_run=%s),
                -- (select * from HF_HOUR)

                -- 各个行驶区间在不同工作日类型的不同小时段的表现
                HF_WEEK_HOUR AS
                (select 
                    *
                from 
                    algorithm_db.bapt_knowledge_weekhour
                where route_id_run=%s),
                -- (select * from HF_WEEK_HOUR)

                -- 各个行驶区间在节假日与工作日的表现
                HF_HOLIDAY AS
                (select 
                    *
                from 
                    algorithm_db.bapt_knowledge_holiday
                where route_id_run=%s),
                -- (select * from HF_HOLIDAY)

                -- 不同行驶区间的历史表现情况
                -- 不同区间的平均行驶时长
                -- 不同行驶区间的高峰小时段
                HF_CONBINE AS
                (select 
                    *
                from
                    algorithm_db.bapt_knowledge_conbine
                where route_id_run=%s),
                -- (SELECT * FROM HF_CONBINE)

                -- 实时特征提取
                -- 最近N趟车特征
                last1_setime_data as
                (select route_id_run route_id,conbine_id,mid_num,direction,
                        minute5_parti,minute10_parti,minute15_parti,minute30_parti, hour_parti,
                        week_parti,holiday,label,
                        (unix_timestamp(lasti_end_time)-unix_timestamp(lasti_start_time)) last1_waitingtime,
                        start_time,triplog_id
                from 
                    R
                where rank_number = 1),
                -- (select COUNT(*) from last1_setime_data)

                last2_setime_data as
                (select route_id_run route_id,conbine_id,mid_num,direction,
                        minute5_parti,minute10_parti,minute15_parti,minute30_parti, hour_parti,
                        week_parti,holiday,label,
                        (unix_timestamp(lasti_end_time)-unix_timestamp(lasti_start_time)) last2_waitingtime,
                        start_time,triplog_id
                from 
                    R
                where rank_number = 2),
                -- (select * from last2_setime_data)

                last3_setime_data as
                (select route_id_run route_id,conbine_id,mid_num,direction,
                        minute5_parti,minute10_parti,minute15_parti,minute30_parti, hour_parti,
                        week_parti,holiday,label,
                        (unix_timestamp(lasti_end_time)-unix_timestamp(lasti_start_time)) last3_waitingtime,
                        start_time,triplog_id
                from 
                    R
                where rank_number = 3),
                -- (select * from last3_setime_data)

                last4_setime_data as
                (select route_id_run route_id,conbine_id,mid_num,direction,
                        minute5_parti,minute10_parti,minute15_parti,minute30_parti, hour_parti,
                        week_parti,holiday,label,
                        (unix_timestamp(lasti_end_time)-unix_timestamp(lasti_start_time)) last4_waitingtime,
                        start_time,triplog_id
                from 
                    R
                where rank_number = 4),
                -- (select * from last4_setime_data)

                last5_setime_data as
                (select route_id_run route_id,triplog_id,conbine_id,start_time,
                        mid_num,direction,
                        minute5_parti,minute10_parti,minute15_parti,minute30_parti, hour_parti,
                        week_parti,holiday,label,
                        (unix_timestamp(lasti_end_time)-unix_timestamp(lasti_start_time)) last5_waitingtime
                from 
                    R
                where rank_number = 5),
                -- (select COUNT(*) from last5_setime_data)

                -- 提取最近5趟车时长
                last1_5_setime_data as
                (SELECT l1.*,l2.last2_waitingtime, l3.last3_waitingtime,l4.last4_waitingtime,l5.last5_waitingtime
                from 
                    last1_setime_data l1 JOIN last2_setime_data l2 
                    on ((l1.route_id=l2.route_id) and (l1.conbine_id=l2.conbine_id) and (l1.triplog_id=l2.triplog_id) and (l1.start_time=l2.start_time))
                    JOIN last3_setime_data l3 
                    on ((l1.route_id=l3.route_id) and (l1.conbine_id=l3.conbine_id) and (l1.triplog_id=l3.triplog_id) and (l1.start_time=l3.start_time))
                    JOIN last4_setime_data l4 
                    on ((l1.route_id=l4.route_id) and (l1.conbine_id=l4.conbine_id) and (l1.triplog_id=l4.triplog_id) and (l1.start_time=l4.start_time))
                    JOIN last5_setime_data l5 
                    on ((l1.route_id=l5.route_id) and (l1.conbine_id=l5.conbine_id) and (l1.triplog_id=l5.triplog_id) and (l1.start_time=l5.start_time))
                    ),
                -- (select * from last1_5_setime_data)

                -- (select route_id,conbine_id,mid_num,direction,holiday,start_time,week_parti,hour_parti,
                --         last1_waitingtime,last2_waitingtime,last3_waitingtime,last4_waitingtime,last5_waitingtime,label
                -- from last1_5_setime_data)

                --*填充缺失值（用历史同比均值填充）

                -- 求最后5趟车的时序特征
                last1_5_feature_data as
                (select last1_5_setime_data.*, 
                        (last1_waitingtime+last2_waitingtime+last3_waitingtime+last4_waitingtime+last5_waitingtime)/5 avg_last, --最后5趟车的平均值
                        (last1_waitingtime-last2_waitingtime) diff_12,(last2_waitingtime-last3_waitingtime) diff_23,(last3_waitingtime-last4_waitingtime) diff_34,(last4_waitingtime-last5_waitingtime) diff_45,  --前一趟车与后一趟车的行驶时长差值（环比）
                        (last1_waitingtime-last2_waitingtime)/last2_waitingtime slop_12,(last2_waitingtime-last3_waitingtime)/last3_waitingtime slop_23,(last3_waitingtime-last4_waitingtime)/last4_waitingtime slop_34,(last4_waitingtime-last5_waitingtime)/last5_waitingtime slop_45 --前一趟车与后一趟车的斜率
                        -- 历史同比差值
                from last1_5_setime_data),
                -- (select * from last1_5_feature_data)

                -- 合并历史特征群
                -- (select 
                --     ld.*,HF_CONBINE.conbine_avg,HF_CONBINE.conbine_max,HF_CONBINE.conbine_min,HF_CONBINE.peak_hours
                -- from 
                --     last1_5_feature_data ld
                -- left join 
                --     HF_CONBINE
                -- on ((ld.route_id_run=HF_CONBINE.route_id_run) and (ld.conbine_id=HF_CONBINE.conbine_id) and (ld.mid_num=HF_CONBINE.mid_num))
                -- )

                -- 合并历史特征群
                add_history_data as
                (select 
                    ld.*,
                    HF_CONBINE.conbine_peak_hours,HF_CONBINE.conbine_avg,HF_CONBINE.conbine_max,HF_CONBINE.conbine_min,
                    HF_WEEK.week_avg,HF_WEEK.week_max,HF_WEEK.week_min,HF_WEEK.week_triplog_num,
                    HF_HOUR.hour_avg,HF_HOUR.hour_max,HF_HOUR.hour_min,
                    HF_WEEK_HOUR.weekhour_avg,HF_WEEK_HOUR.weekhour_max,HF_WEEK_HOUR.weekhour_min,
                    HF_HOLIDAY.holiday_avg,HF_HOLIDAY.holiday_max,HF_HOLIDAY.holiday_min,HF_HOLIDAY.holiday_triplog_num
                from
                    last1_5_feature_data ld
                left join HF_CONBINE on ((ld.route_id=HF_CONBINE.route_id_run) and (ld.conbine_id=HF_CONBINE.conbine_id) and (ld.mid_num=HF_CONBINE.mid_num))
                left join HF_WEEK on ((ld.route_id=HF_WEEK.route_id_run) and (ld.conbine_id=HF_WEEK.conbine_id) and (ld.mid_num=HF_WEEK.mid_num) and (ld.week_parti=HF_WEEK.week_parti))
                left join HF_HOUR on ((ld.route_id=HF_HOUR.route_id_run) and (ld.conbine_id=HF_HOUR.conbine_id) and (ld.mid_num=HF_HOUR.mid_num) and (ld.hour_parti=HF_HOUR.hour_parti))
                left join HF_WEEK_HOUR on ((ld.route_id=HF_WEEK_HOUR.route_id_run) and (ld.conbine_id=HF_WEEK_HOUR.conbine_id) and (ld.mid_num=HF_WEEK_HOUR.mid_num) and (ld.week_parti=HF_WEEK_HOUR.week_parti) and (ld.hour_parti=HF_WEEK_HOUR.hour_parti))
                left join HF_HOLIDAY on ((ld.route_id=HF_HOLIDAY.route_id_run) and (ld.conbine_id=HF_HOLIDAY.conbine_id) and (ld.mid_num=HF_HOLIDAY.mid_num) and (ld.holiday=HF_HOLIDAY.holiday))
                ),
                -- (select * from add_history_data)

                -- 判断记录是否在高峰小时段
                inidx_locate_data as
                (select 
                    *, 
                    locate(concat(',',cast(hour_parti as string),','), concat(',',conbine_peak_hours,',')) inidx_locate
                    -- case when concat(',',cast(hour_parti as string),',') in concat(',',conbine_peak_hours,',') then 1
                    -- else 0
                    -- end is_peak_hour
                from  
                    add_history_data),

                -- 提取出该条记录是否在高峰小时段的特征
                add_peak_hour_data as
                (select 
                    *,
                    case when inidx_locate >= 1 then 1 
                    else 0 
                    end is_peak_hour
                from  
                    inidx_locate_data),
                -- (select * from add_peak_hour_data)

                -- 去除无用字段，得到最终的训练数据集
                train_data as
                (select 
                    conbine_id,mid_num,direction,minute5_parti,minute10_parti,minute15_parti,minute30_parti,hour_parti,week_parti,holiday,
                    last1_waitingtime,last2_waitingtime,last3_waitingtime,last4_waitingtime,last5_waitingtime,avg_last,diff_12,diff_23,diff_34,diff_45,slop_12,slop_23,slop_34,slop_45,
                    conbine_avg,conbine_max,conbine_min,week_avg,week_max,week_min,week_triplog_num,hour_avg,hour_max,hour_min,weekhour_avg,weekhour_max,weekhour_min,holiday_avg,holiday_max,holiday_min,holiday_triplog_num,is_peak_hour,label,
                    route_id
                from  
                    add_peak_hour_data)
                -- (select * from train_data)

                -- 把该条线路提取到的特征数据插入到指定的特征训练集表中
                insert into algorithm_db.batp_train_data partition(route_id) select * from train_data"""%("'"+route_idi+"'","'"+route_idi+"'","'"+route_idi+"'","'"+route_idi+"'","'"+route_idi+"'")
        cur.execute(sql)

#if __name__ == "__main__":
#    # 生成历史特征群表
#    gk = GenerateKnowledge()
#    gk.generate_knowledge('2021-02-09', '2021-03-15')
    
    # 生成训练集
#    gt = GenerateTrainData()
#    gt.generate_all_train_data('2021-02-09', '2021-03-15')