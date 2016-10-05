CREATE EXTERNAL TABLE IF NOT EXISTS vrawload (
ts string,
mid string,
market string,
percentage double,
workdone double 
)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION "hdfs:///user//vagrant//junk//vrawload//";

CREATE EXTERNAL TABLE IF NOT EXISTS vprdload (
mid string,
market string,
hr int,
weekday int,
percentage double,
workdone double
)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION "hdfs:///user//vagrant//junk//vprdload//";
