package pers.yzq.spark.hbase.MovingAverage
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import pers.yzq.spark.{PropertiesHelper, YLogger}
import pers.yzq.spark.hbase.Common

import scala.collection.mutable

/**
  *
  * @Author: YZQ
  * @date 2019/5/21
  */
object MAv1 {

  /**
    * Assume time-creasing windows
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val winStart = PropertiesHelper.getProperty("twa.start").toLong
    @Deprecated
    val winEnd = PropertiesHelper.getProperty("twa.end").toLong
    val winSize = PropertiesHelper.getProperty("twa.win.size").toLong
    val winStep = PropertiesHelper.getProperty("twa.win.step").toLong
    val winLength = PropertiesHelper.getProperty("twa.win.length").toInt
    val minKeepInMem = 1
    val hregions = 10

    val conf = new SparkConf()
      .setAppName("TWA-HBASE-MAv1" + System.currentTimeMillis())
    val sc = new SparkContext(conf)
    val common = new Common

    var winHeader = winStart
    var startTimeStamp = winHeader
    var endTimeStamp = startTimeStamp + winSize
    val winRDDs = new mutable.LinkedHashMap[Int, RDD[(Long, Long)]]
    var midRDD = sc.emptyRDD[Long]

    for(winId <- 0 until winLength) {
      val suffixWRDD = common.trans2DT(common.loadRDD(sc,start = startTimeStamp, end = endTimeStamp))
      YLogger.ylogInfo(this.getClass.getSimpleName)(s"HBase 载入 suffixWRDD 范围 {${startTimeStamp}~${endTimeStamp}}.")
      val prefixWRDD = winRDDs.get(winId - 1) match {
        case Some(rdd) => rdd.filter((a:(Long, Long)) => a._2 >= winHeader)
        case _=>sc.emptyRDD[(Long, Long)]
      }
      val winRDD = prefixWRDD.union(suffixWRDD).coalesce(hregions, false).persist(StorageLevel.MEMORY_ONLY).setName(s"winRDD[${winId}].")
      winRDDs.put(winId, winRDD)
      YLogger.ylogInfo(this.getClass.getSimpleName)(s"窗口RDD [${winRDD.id}] 范围 {${winHeader}~${winHeader + winSize}}.")

      val average = winRDD.map(e => e._1).reduce(_+_) / winSize
      YLogger.ylogInfo(this.getClass.getSimpleName) (s"平均值为 ${average}.")
      winRDDs.get(winId - minKeepInMem) match {
        case Some(rdd) =>
          YLogger.ylogInfo(this.getClass.getSimpleName) (s"窗口RDD[${rdd.id}] 被清除.")
          rdd.unpersist(true)
        case _ =>
      }
      val winAve = sc.parallelize(Seq(average))
      YLogger.ylogInfo(this.getClass.getSimpleName) (s"窗口平均值RDD [${winAve.id}].")
      midRDD = midRDD.union(winAve).persist(StorageLevel.MEMORY_ONLY)
      midRDD.count()

      startTimeStamp = winHeader + Math.max(winSize, winStep)
      endTimeStamp = startTimeStamp + Math.min(winSize, winStep)
      winHeader += winStep
    }
    val nextWinValue = midRDD.reduce(_+_) / winLength
    YLogger.ylogInfo(this.getClass.getSimpleName) (s"聚合窗口平均值的平均值 -> ${nextWinValue}.")
    sc.stop()
  }
}
