#!/bin/bash

export SPARK_MAJOR_VERSION=2
export SPARK_HOME=/usr/hdp/current/spark2-client/

SRCDIR=$( cd -P $(dirname ${BASH_SOURCE[0]}); echo $PWD )

spark-submit \
--master yarn \
--deploy-mode cluster \
--class com.verizon.SpendTaxonomyClassification \
--num-executors 10 \
--executor-memory 8G \
--driver-memory 8G \
--conf spark.driver.maxResultSize=4G \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.dynamicAllocation.maxExecutors=15 \
--conf spark.shuffle.service.enabled=true \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.rdd.compress=true \
--files /usr/hdp/current/spark-client/conf/hive-site.xml \
--jars /usr/hdp/current/spark-client/lib/datanucleus-api-jdo-3.2.6.jar,/usr/hdp/current/spark-client/lib/datanucleus-rdbms-3.2.9.jar,/usr/hdp/current/spark-client/lib/datanucleus-core-3.2.10.jar \
$SRCDIR/target/scala-2.11/SpendProject-assembly-1.0.jar
