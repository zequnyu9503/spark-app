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

import org.apache.spark.{SparkConf, SparkContext}

import org.scalatest.FunSuite

class TimeWindowRDDIteratorSuite extends FunSuite {

  test("TimeWindowRDD Logical Examination") {
    val conf = new SparkConf().setMaster("local").setAppName(s"Exp1")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val itr = new TimeWindowRDD[Long, Integer](
      sc, 10, 20,
      (startTime: Long, endTime: Long) => {
        sc.parallelize(Array.range(startTime.toInt, endTime.toInt), 2)
          .map(e => (e, e))
      }).setKeepInMem(5).setKeepInMemCapacity(8).iterator()

    while (itr.hasNext) {
      val rdd = itr.next()
      // scalastyle:off println
      println(
        s"Elements < ${rdd.map(e => e._2.asInstanceOf[Integer]).collect().mkString(",")} >" +
          s" partitions <${rdd.getNumPartitions}>")
      // scalastyle:on println
    }
  }
}
