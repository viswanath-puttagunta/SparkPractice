CREATE EXTERNAL TABLE IF NOT EXISTS vlocations (
uid string,
loc_ts_dur string,
data_dt int 
)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION "hdfs:///user//vagrant//junk//vlocations//";
