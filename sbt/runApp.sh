#!/bin/bash

export SPARK_MAJOR_VERSION=2
export SPARK_HOME=/usr/hdp/current/spark2-client/
set http_proxy=http://proxy.ebiz.verizon.com:80
set https_proxy=https://proxy.ebiz.verizon.com:80
set ANT_OPTS=-Dhttp.proxyHost=proxy.ebiz.verizon.com -Dhttp.proxyPort=80
SRCDIR=$( cd -P $(dirname ${BASH_SOURCE[0]}); echo $PWD )

/usr/hdp/current/spark2-client/bin/spark-submit \
--master yarn \
--deploy-mode client \
--class com.verizon.EagleStreaming \
--conf spark.driver.maxResultSize=4G \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=2 \
--conf spark.dynamicAllocation.maxExecutors=8 \
--conf spark.shuffle.service.enabled=true \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.rdd.compress=true \
--conf "spark.driver.extraJavaOptions=-Dhttp.proxyHost=proxy.ebiz.verizon.com \
-Dhttp.proxyPort=80 -Dhttps.proxyHost=proxy.ebiz.verizon.com -Dhttps.proxyPort=80" \
--files /usr/hdp/current/spark-client/conf/hive-site.xml \
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0,org.apache.spark:spark-streaming-kafka-0-10_2.11:2.1.0 \
/vzwhome/riopery/eagle/sbt/target/scala-2.11/EagleStreaming-assembly-1.0.jar
