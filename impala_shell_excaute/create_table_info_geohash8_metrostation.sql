
drop table if exists algorithm_db.dwd_common_geohash8_metrostation;

CREATE TABLE algorithm_db.dwd_common_geohash8_metrostation ( 
    geohash5 STRING,  
    geohash8 STRING,   
    g8_lon STRING,  
    g8_lat STRING,  
    mstation_name STRING, 
    mstation_distance INT)
PARTITIONED BY (   geohash4 STRING ) 
STORED AS parquet;

-- select count(1) from algorithm_db.dwd_common_geohash8_metrostation


