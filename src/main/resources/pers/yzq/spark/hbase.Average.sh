#!/bin/bash
libs_dir="hdfs://centos3:9000/spark-libs"
spark-submit \
--master spark://centos3:7079 \
--executor-memory 8g \
--executor-cores 4 \
--driver-cores 4 \
--class pers.yzq.spark.hbase.AverageV0 \
--jars \
${libs_dir}/hbase-common-2.1.4.jar,\
${libs_dir}/hbase-client-2.1.4.jar,\
${libs_dir}/hbase-mapreduce-2.1.4.jar,\
${libs_dir}/hbase-protocol-2.1.4.jar,\
${libs_dir}/hbase-protocol-shaded-2.1.4.jar,\
${libs_dir}/hbase-shaded-miscellaneous-2.1.0.jar,\
${libs_dir}/hbase-shaded-netty-2.1.0.jar,\
${libs_dir}/hbase-shaded-protobuf-2.1.0.jar,\
${libs_dir}/htrace-core4-4.2.0-incubating.jar,\
${libs_dir}/hbase-spark-2.0.0-alpha4.jar,\
${libs_dir}/spark-catalyst_2.11-2.4.0.jar,\
${libs_dir}/spark-core_2.11-1.5.2.logging.jar,\
${libs_dir}/hbase-server-2.1.4.jar \
spark-2.0.jar
