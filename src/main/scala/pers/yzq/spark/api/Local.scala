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

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Local {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local").setAppName("TWA EXP")
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")

    def func(start: Long, end: Long): RDD[(Long, Long)] = {
      if (start < 100) {
        sc.parallelize(Seq(1, 1, 1, 1, 1)).map((e => (e, e)))
      } else {
        sc.emptyRDD[(Long, Long)]
      }
    }

    val itr = new TimeWindowRDD[Long, Long](
      10,
      10,
      func
    ).setScope(0, 100).iterator()

    while (itr.hasNext) {
      val rdd = itr.next()
      val count = rdd.count()
      // scalastyle:off println
      println(s"count is ${count}")
      // scalastyle:on println
    }
  }
}
