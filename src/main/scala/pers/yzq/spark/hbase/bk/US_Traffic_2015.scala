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

package pers.yzq.spark.hbase.bk

import java.net.URI
import java.text.SimpleDateFormat
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import pers.yzq.spark.PropertiesHelper
import pers.yzq.spark.hbase.Common.HBaseCommon.{path, user}

object US_Traffic_2015 extends BulkLoad {

  tableName = PropertiesHelper.getProperty("hbase.bulkload.tablename")
  columnFamily = PropertiesHelper.getProperty("hbase.bulkload.columnfamily")
  columnQualify = PropertiesHelper.getProperty("hbase.bulkload.columnqualify")
  hfile = PropertiesHelper.getProperty("hbase.bulkload.hfile")
  hadoop_file = PropertiesHelper.getProperty("hbase.bulkload.hadoopfile")

  def main(args: Array[String]): Unit = {
//    prepare(true)
//    bulkLoad()
    prepare()
  }

  override def rdd(): RDD[(ImmutableBytesWritable, KeyValue)] = {
    val conf = new SparkConf().setAppName("US_Traffic_2015")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(hadoop_file).persist(StorageLevel.MEMORY_AND_DISK_SER)
    rdd
      .map(e => {
        val params = e.split(",").map(p => p.replaceAll("\"", ""))
        val key = e.hashCode()
        val dateFormat =
          new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(params(0))
        val timestamp: Long = String.valueOf(dateFormat).toLong
        val dataCollection = new Array[String](24)
        Array.copy(params, 13, dataCollection, 0, 24)
        val dataSet = dataCollection.map(e => e.toInt)
        for (i <- Range(0, 24)) {
          // 原始数据为区间, 交通统计采用区间中位数.
          dataSet(i) = dataSet(i) * (i * 100 + 50)
        }
        val trafficCounts: Long = dataSet.sum
        (key, (trafficCounts, timestamp))
      })
      .sortByKey()
      .map(e => {
        val k = new ImmutableBytesWritable(Bytes.toBytes(e._1))
        val kv = new KeyValue(
          Bytes.toBytes(e._1),
          Bytes.toBytes(PropertiesHelper.getProperty("hbase.bulkload.columnfamily")),
          Bytes.toBytes(PropertiesHelper.getProperty("hbase.bulkload.columnqualify")),
          e._2._2,
          Bytes.toBytes(e._2._1))
        (k, kv)
      })
  }

  override def split(): Array[Array[Byte]] = {
    val splitSet = new Array[Array[Byte]](20)
    val set = new util.TreeSet[Array[Byte]](Bytes.BYTES_COMPARATOR)
    for (i <- Range(0, 20))
      set.add(Bytes.toBytes((98 + i).asInstanceOf[Char] + "0000000000"))
    val itr = splitSet.iterator
    while (itr.hasNext) splitSet :+ itr.next()
    splitSet
  }

  def prepare(): Unit = {
    val configuration = new Configuration
    val fileSystem = FileSystem.get(new URI(path), configuration, user)
    if (!fileSystem.exists(new Path(("/data/dot_traffic_2015.txt")))) {
      throw new Exception("no source file")
    }
    if (fileSystem.exists(new Path(("/data/US_Traffic_2015_TV.txt")))) {
      fileSystem.delete(new Path("/data/US_Traffic_2015_TV.txt"), true)
    }
    val conf = new SparkConf().setAppName("US_Traffic_2015_P")
    val sc = new SparkContext(conf)
    val source = sc
      .textFile("/data/dot_traffic_2015.txt")
      .persist(StorageLevel.MEMORY_ONLY)
    val rdd_0 = source.map(e => e.replaceAll("\"", ""))
    val rdd_1 = rdd_0.map(e => e.split(","))
    val rdd_2 = rdd_1.map(e => {
      val newArray = new Array[String](24)
      Array.copy(e, 13, newArray, 0, 24)
      (e(0), newArray)
    })
    val rdd_3 = rdd_2.map(e => {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd").parse(e._1)
      val timestamp: Long = String.valueOf(dateFormat).toLong
      val dataSet = e._2.map(y => y.toLong)
      for (i <- Range(0, 24)) {
        // 原始数据为区间, 交通统计采用区间中位数.
        dataSet(i) = dataSet(i) * (i * 100 + 50)
      }
      (timestamp, dataSet.sum)
    })
    rdd_3.saveAsTextFile("/data/US_Traffic_2015_TV.txt")
    sc.stop()
  }
}
