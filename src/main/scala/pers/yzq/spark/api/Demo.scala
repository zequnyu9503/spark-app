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

package pers.yzq.spark.api

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{CompareOperator, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.{
  BinaryComparator,
  FamilyFilter,
  FilterList,
  QualifierFilter
}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes

import org.apache.spark.{SparkConf, SparkContext}

import pers.yzq.spark.PropertiesHelper

object Demo {

  def main(args: Array[String]): Unit = {

    val hcp = PropertiesHelper.getProperty("hbase.hcp")
    val tableName = PropertiesHelper.getProperty("hbase.tablename")
    val columnFamily = PropertiesHelper.getProperty("hbase.columnfamily")
    val columnQualify = PropertiesHelper.getProperty("hbase.columnqualify")
    val winSize = PropertiesHelper.getProperty("twa.win.size").toLong
    val winStep = PropertiesHelper.getProperty("twa.win.step").toLong

    val conf = new SparkConf().setAppName("Example of TimeWindowRDD")
    val sc = new SparkContext(conf)

    val winIterator = new TimeWindowRDD[Long, Long](
      sc,
      winSize,
      winStep,
      (start: Long, end: Long) => {
        val hc = HBaseConfiguration.create()
        hc.addResource(new Path(hcp))
        hc.set(TableInputFormat.INPUT_TABLE, tableName)
        hc.set(
          TableInputFormat.SCAN,
          TableMapReduceUtil.convertScanToString(
            new Scan()
              .setFilter(new FilterList(
                FilterList.Operator.MUST_PASS_ALL,
                new FamilyFilter(
                  CompareOperator.EQUAL,
                  new BinaryComparator(Bytes.toBytes(columnFamily))),
                new QualifierFilter(
                  CompareOperator.EQUAL,
                  new BinaryComparator(Bytes.toBytes(columnQualify)))
              ))
              .setTimeRange(start, end))
        )
        sc.newAPIHadoopRDD(hc,
                           classOf[TableInputFormat],
                           classOf[ImmutableBytesWritable],
                           classOf[Result])
          .map(e =>
            (Bytes.toLong(e._2.listCells().get(0).getValueArray),
             e._2.listCells().get(0).getTimestamp))
      }
    ).iterator()

    while (winIterator.hasNext) {
      val winRDD = winIterator.next
      val count = winRDD.count
      // scalastyle:off println
      println(s"count is ${count}")
      // scalastyle:on println
    }
  }
}
