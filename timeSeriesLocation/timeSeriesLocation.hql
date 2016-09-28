CREATE EXTERNAL TABLE IF NOT EXISTS rawLocationTime (
uid string,
start_time string,
dur_loc_seq string,
data_dt string
)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION "hdfs:///user//vagrant//junk//timeSeriesLocation//";
