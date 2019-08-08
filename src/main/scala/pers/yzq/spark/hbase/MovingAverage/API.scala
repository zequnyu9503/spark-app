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

package pers.yzq.spark.hbase.MovingAverage

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.{CompareOperator, HBaseConfiguration}
import org.apache.hadoop.hbase.filter.{BinaryComparator, FamilyFilter, FilterList, QualifierFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import pers.yzq.spark.PropertiesHelper
import pers.yzq.spark.api.TimeWindowRDD

object API {

  def main(args: Array[String]): Unit = {

    val winStart = PropertiesHelper.getProperty("twa.start").toLong
    val winEnd = PropertiesHelper.getProperty("twa.end").toLong
    val winSize = PropertiesHelper.getProperty("twa.win.size").toLong
    val winStep = PropertiesHelper.getProperty("twa.win.step").toLong
    @deprecated
    val winLength = PropertiesHelper.getProperty("twa.win.length").toInt
    val hcp = PropertiesHelper.getProperty("hbase.hcp")
    val tableName = PropertiesHelper.getProperty("hbase.tablename")
    val columnFamily = PropertiesHelper.getProperty("hbase.columnfamily")
    val columnQualify = PropertiesHelper.getProperty("hbase.columnqualify")


    val conf = new SparkConf()
    conf.setAppName(s"MA API ${System.currentTimeMillis()}")
    val sc = new SparkContext(conf)
    sc.setLogLevel("FATAL")
    val itr = new TimeWindowRDD[Long, Long](sc, winSize, winStep,
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
                new FamilyFilter(CompareOperator.EQUAL,
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
          classOf[Result]).map(e =>
            (Bytes.toLong(e._2.listCells().get(0).getValueArray),
              e._2.listCells().get(0).getTimestamp))
      }).setScope(winStart, winEnd).iterator()

    var temp = sc.emptyRDD[Long]
    while(itr.hasNext) {
      val rdd = itr.next()
      val average = rdd.map(e => e._2).reduce(_ + _) / winSize
      temp = temp.union(sc.parallelize(Seq(average)))
    }
    // scalastyle:off println
    println(s"res -> ${temp.reduce(_ + _)}")
    // scalastyle:on println
  }
}
