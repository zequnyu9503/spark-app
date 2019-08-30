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

package pers.yzq.spark
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import pers.yzq.spark.hbase.MovingAverage.MAv0

import scala.collection.mutable

class Some extends FunSuite {

  // scalastyle:off println
  test("TableMapReduceUtil") {
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("random"), Bytes.toBytes("data"))
    val str = TableMapReduceUtil.convertScanToString(scan)
    println(str)
  }

  test("Dynamic RDD") {

    dyRDD(100, 20, 20)
    dyRDD(100, 20, 10)
    dyRDD(100, 10, 20)

    def dyRDD(wholeTimeStamp: Long, winStep: Long, winSize: Long): Unit = {
      var winHeader = 0L
      var startTimeStamp = 0L
      var endTimeStamp = winSize
      println("**************************************")
      while (winHeader <= wholeTimeStamp) {
        println(
          s"TW ranges from ${winHeader} to ${winHeader + winSize} " +
            s"data ranges from ${startTimeStamp} to ${endTimeStamp}")
        startTimeStamp = winHeader + Math.max(winSize, winStep)
        endTimeStamp = startTimeStamp + Math.min(winSize, winStep)
        winHeader += winStep
      }
    }
  }

  test("PropertiesHelper") {
    println(s"hdfs.path: ${PropertiesHelper.getProperty("hdfs.path")}")
  }

  test("dynamic RDD") {
    val conf = new SparkConf().setAppName("dynamicRDD").setMaster("local")
    val sc = new SparkContext(conf)
    val winRDDs = new mutable.Queue[RDD[Int]]()
    winRDDs.enqueue(sc.emptyRDD[Int])

    for (i <- Range(0, 5)) {
      val rdd = sc.parallelize(Seq(1, 1, 1))
      val mid = rdd.map(e => e + i).cache()
      winRDDs.enqueue(mid)
      val res = mid.count()
      println(s"res -> ${res}")
      if (winRDDs.length > 1) {
        for (i <- Range(0, winRDDs.length - 1)) {
          val winCachedRDD = winRDDs.dequeue()
          println(s"remove rdd [${winCachedRDD.id}]")
          winCachedRDD.unpersist(false)
        }
      }
    }
  }

  test("about scala") {
    val op = None
    if (op.isDefined) println("defined") else println("not defined")
  }
  // scalastyle:off println
}
