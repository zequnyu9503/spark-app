#!/bin/bash
libs_dir="hdfs://centos3:9000/spark-libs"
hcp="/home/zc/software/hbase-2.1.4/conf/hbase-site.xml"
tableName="TWA-EXAM-5G"
columnFamily="random"
column="data"
target_file="hdfs://centos3:9000/yu/res/TWA-NUMBERCOUNT-HBASE-"
# time windows
winSize=35491657
# movements
winStep=17745829

giga1=36307538
giga2=72243177
giga3=107913905
giga4=142686134
giga5=177458284
giga10=351317895
giga15=525177201
giga20=699037656
giga25=872897296
giga30=1045290202

spark-submit \
--master spark://centos3:7079 \
--executor-memory 8g \
--executor-cores 4 \
--driver-cores 4 \
--class pers.yzq.spark.hbase.AverageX \
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
spark-2.0.jar ${hcp} ${tableName} ${columnFamily} ${column} "${target_file}$(date "+%Y%m%d%H%M%S")" ${giga5} ${winSize} ${winStep}
