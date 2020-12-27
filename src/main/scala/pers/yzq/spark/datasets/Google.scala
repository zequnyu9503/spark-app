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

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
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

//    val origin_1 = sc.textFile("hdfs://node1:9000/google/task_events")
//
//    // task_events time                [0]             {0}
//    // missing info                    [1] <deleted>   { }
//    // job ID                          [2]             {1}
//    // task index                      [3] <not null>  {2}
//    // machine ID                      [4]             {3}
//    // event type                      [5]             {4}
//    // user                            [6] <deleted>   { }
//    // scheduling class                [7]             {5}
//    // priority                        [8]             {6}
//    // CPU request                     [9]             {7}
//    // memory request                  [10]            {8}
//    // disk space request              [11]            {9}
//    // different machines restriction  [12]            {10}
//
//    // 删除第1&6列.
//    val deleted_1 = origin_1.map(_.split(",", -1)).
//      filter(l => l(0).length <= 19).
//      map(l => (l(0).toLong, l(2), l(3), l(4), l(5), l(7), l(8), l(9), l(10), l(11), l(12)))
//    // 过滤空数据.
//    val filtered_1 = deleted_1.filter(l => l._3 != null)
//    // 按照Job Id分组.
//    val res_1 = filtered_1.groupBy(f = v => (v._2, v._3)).flatMap {
//      jt =>
//        val status = jt._2.toArray.sortBy(_._1)
//        val updated = new ArrayBuffer[String]()
//        for (index <- status.indices) {
//          if (status(index)._5 == STR_EVENT_TYPE_SCHEDULE) {
//            val start = status(index)._1
//            if (index + 1 < status.length) {
//              val m = status(index + 1)
//              if (STR_EVENT_TYPE_OTHERS.contains(m._5)) {
//                updated.append(s"$start,${m._1},${m._2},${m._3},${m._4}" +
//                  s",${m._5},${m._6},${m._7},${m._8},${m._9},${m._10},${m._11}")
//              }
//            }
//          }
//        }
//        updated
//    }
//    res_1.saveAsTextFile("hdfs://node1:9000/google/new_task_events")

    // 这里定义保存的数据结构
    // start time                         [0]
    // end time                           [1]
    // job ID                             [2]
    // task index                         [3]
    // machine ID                         [4]
    // CPU rate                           [5]
    // canonical memory usage             [6]
    // assigned memory usage              [7]
    // unmapped page cache                [8]
    // total page cache                   [9]
    // maximum memory usage               [10]
    // disk I/O time                      [11]
    // local disk space usage             [12]
    // maximum CPU rate                   [13]
    // maximum disk IO time               [14]
    // cycles per instruction             [15]
    // memory accesses per instruction    [16]
    // sample portion                     [17]
    // aggregation type                   [18]
    // sampled CPU usage                  [19]

//    val right = sc.textFile("hdfs://node1:9000/google/task_usage_all.csv").
//      map(_.split(",", -1)).
//      map(l => ((l(2), l(3)), l.mkString(","))).
//      persist(StorageLevel.DISK_ONLY)
//    val left = sc.textFile("hdfs://node1:9000/google/new_task_events").
//      map(_.split(",", -1)).
//      map(l => ((l(2), l(3)), l.mkString(","))).persist(StorageLevel.DISK_ONLY)
//
//    val joined = left.join(right)
//
//    val res = joined.filter(f => {
//      val timescope = f._2._1.split(",")
//      val startTime = timescope(0).toLong
//      val endTime = timescope(1).toLong
//
//      val records = f._2._2.split(",")
//      val startRange = records(0).toLong
//      val endRange = records(1).toLong
//
//      (startTime >= startRange && startTime <= endRange) ||
//        (endTime >= startRange && endTime <= endRange)
//    })
//
//    res.saveAsTextFile("hdfs://node1:9000/google/new_task_usage")


    val res_2 = sc.textFile("hdfs://node1:9000/google/new_task_usage").
      map(l => l.substring(1, l.length - 1).split(",", -1)).
      map(l => {
        val key = l(0).substring(1, l.length - 1).split(",", -1)
        val v = l(1).substring(1, l.length - 1).split(",", -1)
        // path1的所有数据项全部保存.
        val s1 = s"${v(0)},${v(1)},${v(2)},${v(3)},${v(4)}," +
          s"${v(5)},${v(6)},${v(7)},${v(8)},${v(9)},${v(10)},${v(11)}"
        // path2的数据项前5列丢弃, 保存剩余15项.
        val s2 = s"${v(17)},${v(18)},${v(19)},${v(20)},${v(21)},${v(22)},${v(23)}" +
          s"${v(24)},${v(25)},${v(26)},${v(27)},${v(28)},${v(29)},${v(30)},${v(31)}"
        (key(0).toLong, key(1).toLong, v(0).toLong, v(1).toLong, s1, s2)
      }).persist(StorageLevel.MEMORY_AND_DISK)
    // 获取所有JobID, 感觉起始时间排序, 并按照JobId分组保存.
    val jobs = res_2.map(_._1).collect()
    jobs.foreach(j => {
      val rdd = res_2.filter(_._1 == j).sortBy(_._2)
      rdd.map(_._5).coalesce(1).
        saveAsTextFile(s"hdfs://node1:9000/google/res/job_$j")
    })
    // 获取JobId_taskId_startTime_endTime分组保存.
    val jtses = res_2.map(m => (m._1, m._2, m._3, m._4)).collect()
    jtses.foreach(r => {
      val rdd = res_2.filter(m => m._1 == r._1 && m._2 == r._2 && m._3 == r._3 && m._4 == r._4)
      rdd.map(_._6).coalesce(1).
        saveAsTextFile(s"hdfs://node1:9000/google/res/${r._1}_${r._2}_${r._3}_${r._4}")
    })
    res_2.unpersist(true)
    sc.stop()
  }
}
