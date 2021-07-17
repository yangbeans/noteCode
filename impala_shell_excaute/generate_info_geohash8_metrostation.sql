-- create table algorithm_db.dwd_common_geohash8_metrostation as
with t3 as
(SELECT 
    a.*,
    b.station_name,
    b.longitude mlongitude,
    b.latitude mlatitude,
    idatatech_db.dist_diff(cast(a.longitude as string),cast(a.latitude as string),cast(b.longitude as string),cast(b.latitude as string)) mstation_distance
from 
    default.dwd_common_geohash8_4regions a
    -- (select * from default.dwd_common_geohash8_4regions limit 100) a
LEFT JOIN
    idatatech_db.xls_metro_station_location b on 1=1
-- WHERE geohash4='ws0q'   -- 花都 'ws0q' 天河 'ws0e'
where geohash4 = '${var:geohash4}'
),
-- (select * from t3)

t4 as(
select 
    c.*,
    rank() over (partition by geohash8 order by station_name,mstation_distance asc) rn
from 
    (select 
        geohash5,
        geohash8,
        longitude,
        latitude,
        station_name,
        case  -- 如果距离小于0（求距离公式中经纬度为空，求距离异常时），让距离为无穷大，即比对最小距离时忽略没有经纬度的站台
            when mstation_distance < 0 then 9999999
            else mstation_distance
            end mstation_distance,
        geohash4
    from 
        t3
    where
        mstation_distance >= 0
    ) c
),
-- (select * from t4)

geohash8_metrostation_infoi as 
(select 
    geohash5,
    geohash8,
    cast(longitude as string) g8_lon,
    cast(latitude as string) g8_lat,
    station_name mstation_name,
    mstation_distance,
    geohash4
from 
    t4 
where 
    rn=1
)
-- (select * from geohash8_metrostation_infoi)

insert into algorithm_db.dwd_common_geohash8_metrostation partition(geohash4) select * from geohash8_metrostation_infoi

-- show create table algorithm_db.dwd_common_geohash8_metrostation

-- insert into algorithm_db.dwd_common_geohash8_metrostation partition(geohash4) select * from geohash8_metrostation_infoi limit 100

-- select * from idatatech_db.xls_metro_station_location where station_name = '花都广场'

-- select * from default.dwd_common_geohash8_4regions where maxname = '天河区'