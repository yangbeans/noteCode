drop table if exists algorithm_db.dwd_common_geohash8_busstation;

CREATE TABLE algorithm_db.dwd_common_geohash8_busstation ( 
    geohash5 STRING, 
    geohash8 STRING,  
    g8_lon DOUBLE,  
    g8_lat DOUBLE,  
    bstation_id STRING,  
    bstation_name STRING,  
    bstation_distance INT) 
PARTITIONED BY (   geohash4 STRING ) 
STORED AS parquet;