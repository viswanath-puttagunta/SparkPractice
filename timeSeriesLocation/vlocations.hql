CREATE EXTERNAL TABLE IF NOT EXISTS vlocations (
uid string,
loc string,
ts int,
dur int
)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION "hdfs:///user//vagrant//junk//vlocations//";
