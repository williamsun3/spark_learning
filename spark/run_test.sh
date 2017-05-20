#!/bin/bash

#remove existing HDFS folders
hdfs dfs -rm -R /user/events/tmp
hdfs dfs -rm -R /user/events/test/data
hdfs dfs -rm -R /user/events/test/schema

#submit the job
spark-submit --class ETL_Test --master yarn-cluster --num-executors 2 --driver-memory 512m --executor-memory 512m --executor-cores 1 --jars /usr/hdp/current/hbase-client/lib/hbase-client.jar,/usr/hdp/current/hbase-client/lib/hbase-common.jar,/usr/hdp/current/hbase-client/lib/hbase-server.jar,/usr/hdp/current/hbase-client/lib/guava-12.0.1.jar,/usr/hdp/current/hbase-client/lib/htrace-core-3.1.0-incubating.jar --files /usr/hdp/current/hbase-client/conf/hbase-site.xml target/scala-2.10/event-etls_2.10-1.0.jar




