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

import java.io.File
import java.nio.charset.Charset

import com.google.common.io.Files
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

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

//    val source = Source.fromFile("/opt/zequnyu/result_1/list.txt", "UTF-8")
//    val lines = source.getLines().toArray
//    val handled = lines.map(_.split("_")(1))
//
//    val origin_1 = sc.textFile("hdfs://node1:9000/google/task_events")

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

//    // 删除第1&6列.
//    val deleted_1 = origin_1.map(_.split(",", -1)).
//      filter(l => l(0).length <= 19).
//      map(l => (l(0).toLong, l(2), l(3), l(4), l(5), l(7), l(8), l(9), l(10), l(11), l(12)))
//    // 过滤空数据.
//    val filtered_1 = deleted_1.filter(l => l._3 != null)
//    // 按照Job Id分组.
//    val res_1: RDD[(Long, String)] =
//      filtered_1.groupBy(f = v => (v._2, v._3)).flatMap {
//      jt =>
//        val status = jt._2.toArray.sortBy(_._1)
//        val updated = new ArrayBuffer[(Long, String)]()
//        for (index <- status.indices) {
//          if (status(index)._5 == STR_EVENT_TYPE_SCHEDULE) {
//            val start = status(index)._1
//            if (index + 1 < status.length) {
//              val m = status(index + 1)
//              if (STR_EVENT_TYPE_OTHERS.contains(m._5)) {
//                updated.append((jt._1._1.toLong, s"$start,${m._1},${m._2},${m._3},${m._4}" +
//                  s",${m._5},${m._6},${m._7},${m._8},${m._9},${m._10},${m._11}"))
//              }
//            }
//          }
//        }
//        updated
//    }.sortBy(_._1)

//    res_1.saveAsTextFile("hdfs://node1:9000/google/res_1.txt")

//    val res_2 = res_1.groupBy(_._1).map(job => {
//      val jobId = job._1
//      val records = job._2.
//        toArray.
//        sortBy(_._2).
//        map(_._3).
//        reduce(_ + "\n" + _)
//      jobId + "|" + records
//    })

//    res_2.saveAsTextFile("hdfs://node1:9000/google/origin_1.txt")

//
//    val jobs = res_1.map(_._1).distinct().collect().filter(job => {
//      !handled.contains(job)
//    })
//
//    jobs.foreach(job => {
//      val f = new File(s"/opt/zequnyu/result_1/job_${job}")
//      val toBeSaved = res_1.
//        filter(_._1 == job).
//        sortBy(_._2).
//        map(_._3).
//        distinct().
//        reduce(_ + "\n" + _)
//      Files.write(toBeSaved.getBytes, f)
//    })
//


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

    val right = sc.textFile("hdfs://node1:9000/google/task_usage_all.csv").
      map(_.split(",", -1)).
      map(l => ((l(2), l(3)), l.mkString(","))).
      persist(StorageLevel.DISK_ONLY)
    val left = sc.textFile("hdfs://node1:9000/google/res_1.txt").
      map(_.replace("[(|)]", "")).
      map(_.split(",", -1)).
      map(l => ((l(3), l(4)), (l(1).toLong, l(2).toLong))).
      persist(StorageLevel.MEMORY_ONLY_SER)

    val joined = left.join(right)

    val res = joined.filter(f => {
      val va: ((Long, Long), String) = f._2
      val time = va._1

      val startTime = time._1
      val endTime = time._2

      val records = f._2._2.split(",", -1)
      val startRange = records(0).toLong
      val endRange = records(1).toLong

      (startTime >= startRange && startTime <= endRange) ||
        (endTime >= startRange && endTime <= endRange)
    }).map(f => {
      val d = f._2._2.split(",", -1)
      // 前5列数据丢弃, 构造Key作为第一个.
      val vals = s"${d(5)},${d(6)}," +
        s"${d(7)},${d(8)},${d(9)},${d(10)},${d(11)}," +
        s"${d(12)},${d(13)},${d(14)},${d(15)},${d(16)}," +
        s"${d(17)},${d(18)},${d(19)}"
      (s"${d(2)}-${d(3)}-${d(0)}-${d(1)}", vals)
    }).sortBy(_._1).map(f => s"${f._1}|${f._2}")

    res.saveAsTextFile("hdfs://node1:9000/google/new_task_usage")
  }
}
