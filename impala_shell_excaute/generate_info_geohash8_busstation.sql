with t1 as
(SELECT 
    a.*,
    b.station_name,
    b.station_id,
    idatatech_db.dist_diff(cast(a.longitude as string),cast(a.latitude as string),cast(b.longituded as string),cast(b.latituded as string)) bstation_distance
from 
    default.dwd_common_geohash8_4regions a
    -- (select * from default.dwd_common_geohash8_4regions limit 100) a
LEFT JOIN
    idatatech_db.xls_bus_station_location b on 1=1
-- WHERE geohash4='ws0q'
where geohash4 = '${var:geohash4}'
),
-- (select * from t1 where bstation_distance <= 0)

t2 as(
select 
    c.*,
    rank() over (partition by geohash8 order by station_id,bstation_distance asc) rn
from 
    (select 
        geohash5,geohash8,
        longitude,
        latitude,
        station_id,
        station_name,
        case -- 如果距离小于0（求距离公式中经纬度为空，求距离异常时），让距离为无穷大，即比对最小距离时忽略没有经纬度的站台
            when bstation_distance < 0 then 9999999
            else bstation_distance
            end bstation_distance,
        geohash4
    from 
        t1
    ) c
),
-- (select * from t2)

geohash8_busstation_infoi as 
(select 
    geohash5,
    geohash8,
    cast(longitude as string) g8_lon,
    cast(latitude as string) g8_lat,
    station_id bstation_id,
    station_name bstation_name,
    bstation_distance,
    geohash4
from 
    t2 
where 
    rn=1
)
-- (select * from geohash8_busstation_infoi)

insert into algorithm_db.dwd_common_geohash8_busstation partition(geohash4) select * from geohash8_busstation_infoi