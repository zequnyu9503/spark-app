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
    val exclude = Array(
      6458245869L,
      6432181145L,
      6433636445L,
      6337545460L,
      6312030784L,
      6456753080L,
      6303170284L,
      6424006387L,
      6225099547L,
      6445801042L,
      6362687634L,
      6349314321L,
      6238340468L,
      6376975715L,
      5921809619L,
      6426386351L,
      6278957267L,
      6256014611L,
      6443360499L,
      6387968411L,
      6331396378L,
      6411432337L,
      6276226189L,
      6388672237L,
      6367650972L,
      6430902339L,
      6232605866L,
      6337309061L,
      6393174233L,
      6292289141L,
      6455955021L,
      6339160627L,
      6426750415L,
      6446743402L,
      6482026078L,
      6361591653L,
      6368808489L,
      6419322512L,
      6349661756L,
      6293016992L,
      5546501684L,
      6335329463L,
      6273211450L,
      6344594806L,
      6343160230L,
      6356867509L,
      6397857072L,
      6337197528L,
      6474357415L,
      6308462979L,
      6370078467L,
      5402488769L,
      6330981257L,
      6318602032L,
      6394774480L,
      6433744843L,
      6407155159L,
      6468875195L,
      4392480606L,
      6398069205L,
      6429338816L,
      6318029819L,
      6459360111L,
      6402369371L,
      6308244923L,
      6406719694L,
      6255532702L,
      6463757428L,
      6286038435L,
      6317845421L,
      6410500841L,
      6232606948L,
      6326218840L,
      6277853776L,
      6303838240L,
      6410932746L,
      6249834537L,
      6257655536L,
      6407717890L,
      6344325157L,
      6324858588L,
      6330399423L,
      5285926325L,
      5664371117L,
      6457170908L,
      6461364680L,
      6309204451L,
      6384441943L,
      6276938766L,
      6443039785L,
      6415711953L,
      6311502705L,
      6219557576L,
      6218406404L,
      6266748767L,
      6467588751L,
      6278624411L,
      6281240782L,
      6427926442L,
      4707865726L,
      6422655025L,
      6481404797L,
      6449486668L,
      6356358552L,
      6451841550L,
      6299019734L,
      6298236578L,
      6467223378L,
      6449479809L,
      6388792937L,
      6369604229L,
      6405910961L,
      6339165820L,
      6482155487L,
      6428936517L,
      6392217549L,
      6270648320L,
      6482972832L,
      6446783844L,
      6297902624L,
      6340211531L,
      6331317370L,
      6331417954L,
      6353179873L,
      6330683245L,
      6333365616L,
      6348241620L,
      6411167870L,
      6328414298L,
      6329268340L,
      6378174340L,
      6176871439L,
      6394466047L,
      6387450774L,
      6183750753L,
      5966089485L,
      6337396508L,
      6273527780L,
      6340942691L,
      6413913875L,
      6450055214L,
      6378134218L,
      6340799472L,
      6482161182L,
      6399248021L,
      6282507089L,
      6397960624L,
      6278069248L,
      6460303832L,
      6221861800L,
      6329798860L,
      6383626480L,
      6475883396L,
      6336915376L,
      6303476875L,
      6377987575L,
      6402503934L,
      1412625411L,
      6271789353L,
      6345217785L,
      6331483185L,
      6336594489L,
      6302984780L,
      515042969L,
      6280685099L,
      6483748490L,
      6272793742L,
      6355299289L,
      6376389457L,
      6394989445L,
      6449919400L,
      6279795024L,
      6440303331L,
      6114773114L,
      6393674333L,
      6408568553L,
      6398538665L,
      6348648868L,
      6409132984L,
      6403257931L,
      6418398595L,
      6451650718L,
      6414612306L,
      6291922398L,
      6455428030L,
      6443278354L,
      6440289477L,
      6258695096L,
      6463752348L,
      6253989692L,
      6432179663L,
      6252901355L,
      6337022710L,
      6405993238L,
      5912313637L,
      6316406427L,
      6386187830L,
      6450645546L,
      6484189122L,
      6433818710L,
      6286533506L,
      6342348917L,
      6408673241L,
      6476108349L,
      6463266596L,
      6484723328L,
      6279676768L,
      6385613634L,
      6355568193L,
      6316259781L,
      6356762396L,
      6414778640L,
      6360965938L,
      6429825058L,
      6328439554L,
      6277064893L,
      6392446357L,
      6423025345L,
      6254869357L,
      6454369144L,
      6483991792L,
      6408867552L,
      6408741096L,
      6484993276L,
      6275903028L,
      6287456328L,
      6312699084L,
      6412281336L,
      6324715443L,
      6366275247L,
      6474739255L,
      6322689242L,
      6405537326L,
      6333482222L,
      6366583862L,
      6484359944L,
      6257669344L,
      6338838784L,
      6431571292L,
      6437945704L,
      6411813139L,
      6265003926L,
      6360734949L,
      6453665725L,
      6440307320L,
      6434972012L,
      6277511984L,
      6176858948L,
      5567180387L,
      6446679759L,
      5950955866L,
      6315944449L,
      6383621641L,
      6413182513L,
      6339366132L,
      6370073427L,
      6253644542L)
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