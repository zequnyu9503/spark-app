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

import java.io.File

import com.google.common.io.Files
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

    val local = new File("/home/zc/Documents/size.y")
    val hcp = PropertiesHelper.getProperty("hbase.hcp")
    val tableName = PropertiesHelper.getProperty("hbase.tablename")
    val columnFamily = PropertiesHelper.getProperty("hbase.columnfamily")
    val day_of_data = "day_of_data"
    val month_of_data = "month_of_data"

    val conf = new SparkConf().setAppName("Data_Size_Statistic")
    val sc = new SparkContext(conf)

    for( d <- Range(0, 6)) {
      val dayStart = d * 7 + 1
      val dayEnd = dayStart + 6
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.addResource(hcp)
      hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
      hbaseConf.set(
        TableInputFormat.SCAN,
        TableMapReduceUtil.convertScanToString(
          new Scan().setFilter(new FilterList(
            FilterList.Operator.MUST_PASS_ALL,
            new SingleColumnValueFilter(Bytes.toBytes(columnFamily),
              Bytes.toBytes(month_of_data),
              CompareOperator.GREATER_OR_EQUAL,
              Bytes.toBytes(1)),
            new SingleColumnValueFilter(Bytes.toBytes(columnFamily),
              Bytes.toBytes(day_of_data),
              CompareOperator.GREATER_OR_EQUAL,
              Bytes.toBytes(dayStart)),
            new SingleColumnValueFilter(Bytes.toBytes(columnFamily),
              Bytes.toBytes(day_of_data),
              CompareOperator.LESS_OR_EQUAL,
              Bytes.toBytes(dayEnd))
          )))
      )

      val rdd = sc.newAPIHadoopRDD(hbaseConf,
        classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],
        classOf[Result])
        .map(e => (d, e._2)).cache()
      val co = rdd.count()
      val used_mem = sc.getRDDStorageInfo.find(_.id == rdd.id) match {
        case Some(rDDInfo) => rDDInfo.memSize
        case _ => 0L
      }
      val b = new StringBuilder()
      b.append(s"RDD[${rdd.id}]")
      b.append(s"{${co}}")
      b.append(s"<${used_mem}>")
      Files.write(Bytes.toBytes(b.toString()), local)
    }
  }
}
