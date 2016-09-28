CREATE EXTERNAL TABLE IF NOT EXISTS vrawsession (
uid string,
start_time string,
dur_loc_seq string,
data_dt string
)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION "hdfs:///user//vagrant//junk//vrawsession//";

CREATE EXTERNAL TABLE IF NOT EXISTS vconsolsession (
uid string,
loc_ts_dur string,
data_dt int 
)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION "hdfs:///user//vagrant//junk//vconsolsession//";

CREATE EXTERNAL TABLE IF NOT EXISTS vprdloc (
uid string,
loc string,
prd int,
dur int,
ts int
)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION "hdfs:///user//vagrant//junk//vprdloc//";
