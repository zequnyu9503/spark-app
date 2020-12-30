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

    val maxJobId = "6336594489"
    val exclude = Array(6458245869, 6432181145, 6433636445, 6337545460, 6312030784, 6456753080,6303170284, 6424006387, 6225099547, 6445801042, 6362687634, 6349314321, 6238340468, 6376975715, 5921809619, 6426386351, 6278957267, 6256014611, 6443360499, 6387968411, 6331396378, 6411432337, 6276226189, 6388672237, 6367650972, 6430902339, 6232605866, 6337309061, 6393174233, 6292289141, 6455955021, 6339160627, 6426750415, 6446743402, 6482026078, 6361591653, 6368808489, 6419322512, 6349661756, 6293016992, 5546501684, 6335329463, 6273211450, 6344594806, 6343160230, 6356867509, 6397857072, 6337197528, 6474357415, 6308462979, 6370078467, 5402488769, 6330981257, 6318602032, 6394774480, 6433744843, 6407155159, 6468875195, 4392480606, 6398069205, 6429338816, 6318029819, 6459360111, 6402369371, 6308244923, 6406719694, 6255532702, 6463757428, 6286038435, 6317845421, 6410500841, 6232606948, 6326218840, 6277853776, 6303838240, 6410932746, 6249834537, 6257655536, 6407717890, 6344325157, 6324858588, 6330399423, 5285926325, 5664371117, 6457170908, 6461364680, 6309204451, 6384441943, 6276938766, 6443039785, 6415711953, 6311502705, 6219557576, 6218406404, 6266748767, 6467588751, 6278624411, 6281240782, 6427926442, 4707865726, 6422655025, 6481404797, 6449486668, 6356358552, 6451841550, 6299019734, 6298236578, 6467223378, 6449479809, 6388792937, 6369604229, 6405910961, 6339165820, 6482155487, 6428936517, 6392217549, 6270648320, 6482972832, 6446783844, 6297902624, 6340211531, 6331317370, 6331417954, 6353179873, 6330683245, 6333365616, 6348241620, 6411167870, 6328414298, 6329268340, 6378174340, 6176871439, 6394466047, 6387450774, 6183750753, 5966089485, 6337396508, 6273527780, 6340942691, 6413913875, 6450055214, 6378134218, 6340799472, 6482161182, 6399248021, 6282507089, 6397960624, 6278069248, 6460303832, 6221861800, 6329798860, 6383626480, 6475883396, 6336915376, 6303476875, 6377987575, 6402503934, 1412625411, 6271789353, 6345217785, 6331483185, 6336594489, 6302984780, 515042969, 6280685099, 6483748490, 6272793742, 6355299289, 6376389457, 6394989445, 6449919400, 6279795024, 6440303331, 6114773114, 6393674333, 6408568553, 6398538665, 6348648868, 6409132984, 6403257931, 6418398595, 6451650718, 6414612306, 6291922398, 6455428030, 6443278354, 6440289477, 6258695096, 6463752348, 6253989692, 6432179663, 6252901355, 6337022710, 6405993238, 5912313637, 6316406427, 6386187830, 6450645546, 6484189122, 6433818710, 6286533506, 6342348917, 6408673241, 6476108349, 6463266596, 6484723328, 6279676768, 6385613634, 6355568193, 6316259781, 6356762396, 6414778640, 6360965938, 6429825058, 6328439554, 6277064893, 6392446357, 6423025345, 6254869357, 6454369144, 6483991792, 6408867552, 6408741096, 6484993276, 6275903028, 6287456328, 6312699084, 6412281336, 6324715443, 6366275247, 6474739255, 6322689242, 6405537326, 6333482222, 6366583862, 6484359944, 6257669344, 6338838784, 6431571292, 6437945704, 6411813139, 6265003926, 6360734949, 6453665725, 6440307320, 6434972012, 6277511984, 6176858948, 5567180387, 6446679759, 5950955866, 6315944449, 6383621641, 6413182513, 6339366132, 6370073427, 6253644542).map(_.toLong)
    val right = sc.textFile("hdfs://node1:9000/google/task_usage_all.csv").
      map(f => (f, f.split(",", -1))).
      map(f => ((f._2(2), f._2(3)), f._1)).
      filter(f => !exclude.contains(f._1._1.toLong)).
      persist(StorageLevel.DISK_ONLY)
    val left = sc.textFile("hdfs://node1:9000/google/res_1.txt").
      map(f => f.substring(1, f.length - 1)).
      map(_.split(",", -1)).
      map(l => ((l(3), l(4)), (l(1).toLong, l(2).toLong))).
      filter(f => !exclude.contains(f._1._1.toLong)).
      persist(StorageLevel.MEMORY_ONLY_SER)

    val joined = left.join(right)

    val res = joined.filter(f => {

      val startTime = f._2._1._1
      val endTime = f._2._1._2

      val records = f._2._2.split(",", -1)
      val startRange = records(0).toLong
      val endRange = records(1).toLong

      (startTime >= startRange && startTime <= endRange) ||
        (endTime >= startRange && endTime <= endRange)
    }).map(f => {
        val jobId = f._1._1
        val taskId = f._1._2
        val st = f._2._1._1
        val et = f._2._1._2
        (s"${jobId}-${taskId}-${st}-${et}", f._2._2)
      }).sortBy(_._1).map(f => {
      val d = f._1.split(",", -1)
      // 前5列数据丢弃, 构造Key作为第一个.
      val vals = s"${d(5)},${d(6)}," +
        s"${d(7)},${d(8)},${d(9)},${d(10)},${d(11)}," +
        s"${d(12)},${d(13)},${d(14)},${d(15)},${d(16)}," +
        s"${d(17)},${d(18)},${d(19)}"
      s"${d(2)}-${d(3)}-${d(0)}-${d(1)}|${vals}"
    })

    res.saveAsTextFile("hdfs://node1:9000/google/new_nomax_unsort_task_usage")
  }
}