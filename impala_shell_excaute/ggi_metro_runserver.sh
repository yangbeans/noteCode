export PYTHON_EGG_CACHE=./myeggs
/usr/bin/impala-shell --var=geohash4="hh" -f "create_table_info_geohash8_metrostation.sql"
for geoi in ws0q ws06 ws0v ws03 ws0t ws01 ws0b ws04 ws0m ws0e ws0s ws0k ws0y ws0u ws09 ws0g ws0z ws0x ws0d ws07 ws0c ws0w
do
/usr/bin/impala-shell --var=geohash4="${geoi}" -f "generate_info_geohash8_metrostation.sql"
done