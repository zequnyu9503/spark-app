#!/bin/bash
# 类名.
className=pers.yzq.spark.datasets.Google

spark-submit \
--master spark://node1:7079 \
--executor-memory 24g \
--executor-cores 16 \
--driver-cores 16 \
--class ${className} \
target/spark-2.0.jar