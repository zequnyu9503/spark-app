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

package pers.yzq.spark.datasets

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object Google {

  // 字符常量.
  val STR_EVENT_TYPE_SCHEDULE: String = "1"
  val STR_EVENT_TYPE_SUBMIT: String = "0"

  val STR_EVENT_TYPE_PENDING: String = "7"
  val STR_EVENT_TYPE_RUNNING: String = "8"

  val STR_EVENT_TYPE_OTHERS: Array[String] = Array("2", "3", "4", "5", "6")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Google")
    val sc = new SparkContext(conf)
    sc.setLogLevel("INFO")

    // val origin_1 = sc.textFile("C:\\Users\\yzq\\Desktop\\test.txt")
    val origin_1 = sc.textFile("hdfs://node1:9000/google/task_events")
    val origin_2 = sc.textFile("hdfs://node1:9000/google/task_usage")


    // task_events time                [0]             {0}
    // missing info                    [1] <deleted>   { }
    // job ID                          [2]             {1}
    // task index                      [3] <not null>  {2}
    // machine ID                      [4]             {3}
    // event type                      [5]             {4}
    // user                            [6] <deleted>   { }
    // scheduling class                [7]             {5}
    // priority                        [8]             {6}
    // CPU request                     [9]             {7}
    // memory request                  [10]            {8}
    // disk space request              [11]            {9}
    // different machines restriction  [12]            {10}

    // 删除第1&6列.
    val deleted_1 = origin_1.map(_.split(",", -1)).
      filter(l => l(0).length <= 19).
      map(l => (l(0).toLong, l(2), l(3), l(4), l(5), l(7), l(8), l(9), l(10), l(11), l(12)))
    // 过滤空数据.
    val filtered_1 = deleted_1.filter(l => l._3 != null)
    // 按照Job Id分组.
    val groupByJobId_1 = filtered_1.groupBy(f => f._2)
    val partitioned = groupByJobId_1.repartition(600)
    val jts = partitioned.map(job => {
      // 分组 & 排序 & 处理.
      job._2.groupBy(f => f._3).
        map(task => {
          val status = task._2.toArray.sortBy(_._1)
          val updated = new ArrayBuffer[(Long, Long, String, String, String, String,
            String, String, String, String, String, String)]()
          for (index <- status.indices) {
            if (status(index)._5 == STR_EVENT_TYPE_SCHEDULE) {
              val start = status(index)._1
              if (index + 1 < status.length) {
                val m = status(index + 1)
                if (STR_EVENT_TYPE_OTHERS.contains(m._5)) {
                  updated.append((start, m._1, m._2, m._3, m._4,
                    m._5, m._6, m._7, m._8, m._9, m._10, m._11))
                }
              }
            }
          }
          (job._1, task._1, updated.toArray)
        })
    })
    jts.saveAsTextFile("hdfs://node1:9000/google/new_task_events")
  }

  def decode(key: String): String = {
    key
  }

  def encode(key: String): String = {
    key
  }
}
