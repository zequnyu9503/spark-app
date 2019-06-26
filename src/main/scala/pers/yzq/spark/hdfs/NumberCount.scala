package pers.yzq.spark.hdfs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  *
  * @Author: YZQ
  * @date 2019/5/20
  */
object NumberCount {

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

    val conf = new SparkConf()
      .setAppName("YZQ-TWA-NUMBERCOUNT-HDFS-" + System.currentTimeMillis())
    val sc = new SparkContext(conf)
    var resRDD: RDD[Tuple2[Long, Long]] = sc.emptyRDD[Tuple2[Long, Long]]
    val whoRDD = sc.textFile(source_file).persist(StorageLevel.MEMORY_AND_DISK)
    var cSzie = 0L
    while (cSzie < totalRecords) {
      val winCount = whoRDD
        .map(e => e.split("-"))
        .map(e => (e(0).toLong, e(1).toLong))
        .filter(_._2 >= cSzie)
        .filter(_._2 <= cSzie + winSize)
        .map(e => (e._1, 1L))
        .reduceByKey((a, b) => a + b)
      resRDD = resRDD.union(winCount)
      cSzie += winStep
    }
    resRDD.saveAsTextFile(target_file)
  }
}
