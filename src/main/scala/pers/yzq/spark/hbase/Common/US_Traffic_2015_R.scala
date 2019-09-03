/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pers.yzq.spark.hbase.Common

import org.apache.hadoop.hbase.{CompareOperator, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.{FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import pers.yzq.spark.PropertiesHelper

object US_Traffic_2015_R {
  def main(args: Array[String]): Unit = {

    val hcp = PropertiesHelper.getProperty("hbase.hcp")
    val tableName = PropertiesHelper.getProperty("hbase.tablename")
    val columnFamily = PropertiesHelper.getProperty("hbase.columnfamily")
    val columnQualify = PropertiesHelper.getProperty("hbase.columnqualify")

    val conf = new SparkConf().setAppName("Data_Size_Statistic")
    val sc = new SparkContext(conf)

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.addResource(hcp)
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    hbaseConf.set(
      TableInputFormat.SCAN,
      TableMapReduceUtil.convertScanToString(
        new Scan().setFilter(new FilterList(
          FilterList.Operator.MUST_PASS_ALL,
          new SingleColumnValueFilter(Bytes.toBytes(columnFamily),
                                      Bytes.toBytes(columnQualify),
                                      CompareOperator.GREATER_OR_EQUAL,
                                      Bytes.toBytes(1)),
          new SingleColumnValueFilter(Bytes.toBytes(columnFamily),
                                      Bytes.toBytes(columnQualify),
                                      CompareOperator.LESS_OR_EQUAL,
                                      Bytes.toBytes(7))
        )))
    )

    val rdd = sc.newAPIHadoopRDD(hbaseConf,
                                 classOf[TableInputFormat],
                                 classOf[ImmutableBytesWritable],
                                 classOf[Result])
      .map(e =>
        (Bytes.toLong(e._2.getValue(Bytes.toBytes(columnFamily),
          Bytes.toBytes(columnQualify))), e._2)).cache()
    val co = rdd.count()
  }
}
