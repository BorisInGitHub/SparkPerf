#!/usr/bin/env bash

# Maven clean install
~/Applications/apache-maven-3.3.3/bin/mvn -DskipTests=true clean install

# From the current directory
scp target/hivePerf-1.0-SNAPSHOT-worker.jar mapr@maprdemo:///tmp/


echo "Exécuter sur la machine
hadoop fs -fs maprfs://demo.mapr.com -mkdir -p /user/spark
hadoop fs -fs maprfs://demo.mapr.com -put /opt/mapr/spark/spark-1.6.1/lib/spark-assembly-1.6.1-mapr-1602-hadoop2.7.0-mapr-1602.jar /user/spark/spark-assembly.jar
hadoop fs -fs maprfs://demo.mapr.com -rm /user/spark/hivePerf-1.0-SNAPSHOT-worker.jar
hadoop fs -fs maprfs://demo.mapr.com -put /tmp/hivePerf-1.0-SNAPSHOT-worker.jar /user/spark/hivePerf-1.0-SNAPSHOT-worker.jar
hadoop fs -fs maprfs://demo.mapr.com -chmod -R 777 /user/spark
hadoop fs -fs maprfs://demo.mapr.com -ls /user/spark/

hadoop fs -fs maprfs://demo.mapr.com -put /tmp/dataLong1709858110454081480100.txt /tmp/dataLong1709858110454081480100.txt
hadoop fs -fs maprfs://demo.mapr.com -ls /tmp/
hadoop fs -fs maprfs://demo.mapr.com -rm -r /tmp/spark.pq
hadoop fs -fs maprfs://demo.mapr.com -ls /tmp/
";
