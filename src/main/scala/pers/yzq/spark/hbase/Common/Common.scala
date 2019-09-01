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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.{BinaryComparator, FamilyFilter, FilterList, QualifierFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CompareOperator, HBaseConfiguration}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import pers.yzq.spark.PropertiesHelper

/**
  *
  * @Author: YZQ
  * @date 2019/6/3
  */
class Common {
  def loadRDD(
      sc: SparkContext,
      tableName: String = PropertiesHelper.getProperty("hbase.tablename"),
      columnFamily: String = PropertiesHelper.getProperty("hbase.columnfamily"),
      columnQualify: String =
        PropertiesHelper.getProperty("hbase.columnqualify"),
      start: Long = PropertiesHelper.getProperty("twa.start").toLong,
      end: Long = PropertiesHelper.getProperty("twa.end").toLong)
    : RDD[(ImmutableBytesWritable, Result)] = {
    val hcp = PropertiesHelper.getProperty("hbase.hcp")
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
                       classOf[Result])
  }

  def trans2DT(rdd: RDD[(ImmutableBytesWritable, Result)]) =
    rdd.map(
      e =>
        (Bytes.toLong(e._2.listCells().get(0).getValueArray),
         e._2.listCells().get(0).getTimestamp))

  def trans2D(rdd: RDD[(ImmutableBytesWritable, Result)]) =
    rdd.map(e => Bytes.toLong(e._2.listCells().get(0).getValueArray))
}
