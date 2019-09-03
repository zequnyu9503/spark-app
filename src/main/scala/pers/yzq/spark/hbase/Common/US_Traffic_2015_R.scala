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
import java.nio.charset.StandardCharsets

import com.google.common.io.Files
import org.apache.hadoop.hbase.{CompareOperator, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.{FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object US_Traffic_2015_R {
  def main(args: Array[String]): Unit = {

    val local = new File("/home/zc/Documents/size_m1_per_day")
    val hcp = "/home/zc/software/hbase-2.1.4/conf/hbase-site.xml"
    val tableName = "US_Traffic"
    val columnFamily = "dot_traffic_2015"
    val month_of_data = "month_of_data"
    val day_of_data = "day_of_data"

    val conf = new SparkConf().setAppName("Data_Size_Statistic")
    val sc = new SparkContext(conf)

    val month: Long = 1
    for (day <- Range(0, 30)) {
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
                                        CompareOperator.EQUAL,
                                        Bytes.toBytes(month)),
            new SingleColumnValueFilter(Bytes.toBytes(columnFamily),
                                        Bytes.toBytes(day_of_data),
                                        CompareOperator.EQUAL,
                                        Bytes.toBytes(day))
          )))
      )

      val rdd = sc
        .newAPIHadoopRDD(hbaseConf,
                         classOf[TableInputFormat],
                         classOf[ImmutableBytesWritable],
                         classOf[Result])
        .map(e => (day, e._2))
        .cache()
      val co = rdd.count()
      val used_mem = sc.getRDDStorageInfo.find(_.id == rdd.id) match {
        case Some(rDDInfo) => rDDInfo.memSize
        case _             => 0L
      }
      val b = new StringBuilder()
      b.append(s"RDD[id.${rdd.id}]")
      b.append(s"(2015/${month}/${day})")
      b.append(s"{ele.${co}}")
      b.append(s"<mem.${used_mem}>\r\n")
      Files.append(b.toString(), local, StandardCharsets.UTF_8)
    }
  }
}
