CREATE EXTERNAL TABLE IF NOT EXISTS vlocations (
uid string,
loc_ts_dur string,
data_dt string
)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION "hdfs:///user//vagrant//junk//vlocations//";
