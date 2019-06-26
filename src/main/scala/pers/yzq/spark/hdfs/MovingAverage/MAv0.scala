package pers.yzq.spark.hdfs.MovingAverage

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import pers.yzq.spark.hbase.Common
import pers.yzq.spark.{PropertiesHelper, YLogger}

/**
  *
  * @Author: YZQ
  * @date 2019/5/28
  */
object MAv0 {

  /**
    *  Simple Average.
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val winStart = PropertiesHelper.getProperty("twa.start").toLong
    @Deprecated
    val winEnd = PropertiesHelper.getProperty("twa.end").toLong
    val winSize = PropertiesHelper.getProperty("twa.win.size").toLong
    val winStep = PropertiesHelper.getProperty("twa.win.step").toLong
    val winLength = PropertiesHelper.getProperty("twa.win.length").toInt
    val hdfsPath = PropertiesHelper.getProperty("hdfs.path")

    val conf = new SparkConf().setAppName("TWA-HDFS-MAv0-" + System.currentTimeMillis())
    val sc = new SparkContext(conf)

    var winHeader = winStart
    val wholeRDD = sc.textFile(hdfsPath).
      map(e => e.split("-")).
      map(e => (e(0).toLong, e(1).toLong)).
      persist(StorageLevel.MEMORY_AND_DISK)
    if(wholeRDD == null || wholeRDD.isEmpty()) {
      YLogger.ylogInfo(this.getClass.getSimpleName)(
        "the wholeRDD is null or empty"
      )
      sc.stop()
      System.exit(-1)
    }
    YLogger.ylogInfo(this.getClass.getSimpleName) (s"load hdfs from ${hdfsPath}")
    var midRDD = sc.emptyRDD[Long].persist(StorageLevel.MEMORY_AND_DISK)
    for (i <- Range(0, winLength)) {
      val winRDD = wholeRDD.filter(_._2 >= winHeader).filter(_._2 <= winHeader + winSize)
      YLogger.ylogInfo(this.getClass.getSimpleName)(
        s"winRDD[${winRDD.id}] range from ${winHeader} to ${winHeader + winSize} whose step is ${winStep}.")
      val average = winRDD.map(e => e._1).reduce(_ + _) / winSize
      YLogger.ylogInfo(this.getClass.getSimpleName)(
        s"the average is ${average}")
      val winAve = sc.parallelize(Seq(average))
      YLogger.ylogInfo(this.getClass.getSimpleName)(
        s"create rdd called winAve[${winAve.id}] which calculates average.")
      midRDD = midRDD.union(winAve).persist(StorageLevel.MEMORY_AND_DISK)
      midRDD.count()
      YLogger.ylogInfo(this.getClass.getSimpleName) (
        s"union winAve[${winAve.id}] and midRDD[${midRDD.id}]."
      )
      winHeader += winStep
    }
    val nextWinValue = midRDD.reduce(_ + _) / winLength
    YLogger.ylogInfo(this.getClass.getSimpleName) (s"aggregate rdd of windows for average -> ${nextWinValue}")
    sc.stop()
  }
}
