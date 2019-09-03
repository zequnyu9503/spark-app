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

package pers.yzq.spark.ml

import java.io.File

import com.google.common.io.Files
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.{CompareOperator, HBaseConfiguration}
import org.apache.hadoop.hbase.filter.{FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import pers.yzq.spark.PropertiesHelper
import pers.yzq.spark.api.TimeWindowRDD

object DataSize {

  val local = new File("/home/zc/Documents/size.y")
  val hcp = PropertiesHelper.getProperty("hbase.hcp")
  val tableName = PropertiesHelper.getProperty("hbase.tablename")
  val columnFamily = PropertiesHelper.getProperty("hbase.columnfamily")
  val columnQualify = PropertiesHelper.getProperty("hbase.columnqualify")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Data_Size_Statistic")
    val sc = new SparkContext(conf)
    val twrs = new TimeWindowRDD[Long, Result](
      sc,
      1,
      7,
      (startDay: Long, endDay: Long) => {
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.addResource(hcp)
        hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
        hbaseConf.set(
          TableInputFormat.SCAN,
          TableMapReduceUtil.convertScanToString(
            new Scan().setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,
              new SingleColumnValueFilter(Bytes.toBytes(columnFamily),
                                          Bytes.toBytes(columnQualify),
                                          CompareOperator.GREATER_OR_EQUAL,
                                          Bytes.toBytes(startDay)),
              new SingleColumnValueFilter(Bytes.toBytes(columnFamily),
                                          Bytes.toBytes(columnQualify),
                                          CompareOperator.LESS_OR_EQUAL,
                                          Bytes.toBytes(endDay))))))
        sc.newAPIHadoopRDD(hbaseConf,
                           classOf[TableInputFormat],
                           classOf[ImmutableBytesWritable],
                           classOf[Result])
          .map(e =>
              (Bytes.toLong(e._2.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualify))), e._2))
      })
      .setStorageLevel(StorageLevel.MEMORY_ONLY)
      .setScope(0, 30)
      .iterator()

    while (twrs.hasNext) {
      val timeWindowRDD = twrs.next()
      val records_num = timeWindowRDD.count()
//      val used_mem = sc.getRDDStorageInfo.find(_.id == timeWindowRDD.id) match {
//        case Some(rDDInfo) => rDDInfo.memSize
//        case _ => 0L
//      }
//      val b = new StringBuilder()
//      b.append(s"RDD[${timeWindowRDD.id}]")
//      b.append(s"{${records_num}}")
//      b.append(s"<${used_mem}>")
//      Files.write(Bytes.toBytes(b.toString()), local)
    }
  }
}
