#!/bin/bash
libs_dir="hdfs://centos3:9000/spark-libs"
source_file="hdfs://centos3:9000/yu/data/5G"
target_file="hdfs://centos3:9000/yu/res/TWA-AVERAGE-HDFS-"
# 5 time windows
winSize=35491657
# 5 movements
winStep=35491657

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
--class pers.yzq.spark.hdfs.Average \
spark-2.0.jar ${source_file} "${target_file}$(date "+%Y%m%d%H%M%S")" ${giga5} ${winSize} ${winStep}