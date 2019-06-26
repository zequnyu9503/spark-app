package pers.yzq.spark.hdfs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
  *
  * @Author: YZQ
  * @date 2019/5/15
  */
object Average {
  def main(args: Array[String]): Unit = {
    // Hadoop source file
    val source_file = args(0)
    //Hadoop target file
    val target_file = args(1)
    // Total records
    val totalRecords = args(2).toLong
    // time window size
    val winSize = args(3).toLong
    // time window step
    val winStep = args(4).toLong

    val conf = new SparkConf().setAppName("YZQ-TWA-AVERAGE-HDFS-" + System.currentTimeMillis())
    val sc = new SparkContext(conf)
    var resArray = new ArrayBuffer[Long]()
    var winHeader = 0L

    val wholeRDD = sc.textFile(source_file).persist(StorageLevel.MEMORY_AND_DISK)
    while(winHeader <= totalRecords){
      resArray += wholeRDD.
        map(e => e.split("-")).
        map(e => (e(0).toInt, e(1).toLong)).
        filter(_._2 >= winHeader).
        filter(_._2 <= winHeader + winSize)
        .map(e => e._1).
        reduce(_+_) / winSize

      winHeader += winStep
    }
    sc.parallelize(Seq(resArray)).saveAsTextFile(target_file)
  }
}
