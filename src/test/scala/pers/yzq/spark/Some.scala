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

  test("about replaceAll") {
    val date = "\"2019.9.1\""
    println(s"before ${date} after ${date.replace("\"", "")}")
  }

  test("families") {
    val source = "date,day_of_data,day_of_week,direction_of_travel,direction_of_travel_name,fips_state_code,functional_classification,functional_classification_name,lane_of_travel,month_of_data,record_type,restrictions,station_id,traffic_volume_counted_after_0000_to_0100,traffic_volume_counted_after_0100_to_0200,traffic_volume_counted_after_0200_to_0300,traffic_volume_counted_after_0300_to_0400,traffic_volume_counted_after_0400_to_0500,traffic_volume_counted_after_0500_to_0600,traffic_volume_counted_after_0600_to_0700,traffic_volume_counted_after_0700_to_0800,traffic_volume_counted_after_0800_to_0900,traffic_volume_counted_after_0900_to_1000,traffic_volume_counted_after_1000_to_1100,traffic_volume_counted_after_1100_to_1200,traffic_volume_counted_after_1200_to_1300,traffic_volume_counted_after_1300_to_1400,traffic_volume_counted_after_1400_to_1500,traffic_volume_counted_after_1500_to_1600,traffic_volume_counted_after_1600_to_1700,traffic_volume_counted_after_1700_to_1800,traffic_volume_counted_after_1800_to_1900,traffic_volume_counted_after_1900_to_2000,traffic_volume_counted_after_2000_to_2100,traffic_volume_counted_after_2100_to_2200,traffic_volume_counted_after_2200_to_2300,traffic_volume_counted_after_2300_to_2400,year_of_data"
    val params = source.split(",")
    params.foreach( e => {
      println("\"" + e + "\",")
    })
  }
  // scalastyle:off println
}
