#!/bin/bash
spark-submit \
--class "SimpleApp" \
--master local[2] \
--jars /usr/hdp/2.3.2.0-2950/hbase/lib/hbase-client-1.1.2.2.3.2.0-2950.jar,\
/usr/hdp/2.3.2.0-2950/hive/lib/hive-hbase-handler.jar,\
/usr/hdp/2.3.2.0-2950/hbase/lib/guava-12.0.1.jar,\
/usr/hdp/2.3.2.0-2950/hbase/lib/hbase-common.jar,\
/usr/hdp/2.3.2.0-2950/hbase/lib/hbase-client.jar,\
/usr/hdp/2.3.2.0-2950/hbase/lib/hbase-server.jar,\
/usr/hdp/2.3.2.0-2950/hbase/lib/hbase-protocol.jar,\
/usr/hdp/2.3.2.0-2950/hadoop/client/htrace-core.jar,\
/usr/hdp/2.3.2.0-2950/hadoop/hadoop-common.jar \
target/scala-2.10/simple-project_2.10-1.0.jar
