package pers.yzq.spark.hbase.MovingAverage
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import pers.yzq.spark.{PropertiesHelper, YLogger}
import pers.yzq.spark.hbase.Common

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

    val conf = new SparkConf().setAppName("TWA-HBASE-MAv0-" + System.currentTimeMillis())
    val sc = new SparkContext(conf)
    val common = new Common

    var winHeader = winStart
    var midRDD = sc.emptyRDD[Long].persist(StorageLevel.MEMORY_AND_DISK)
    for (i <- Range(0, winLength)) {
      val winRDD = common.trans2D(common.loadRDD(sc, start = winHeader, end = winHeader + winSize))
      YLogger.ylogInfo(this.getClass.getSimpleName)(s"winRDD[${winRDD.id}] ranges from ${winHeader} to ${winHeader + winSize} " +
        s"fetched data ranges from ${winHeader} to ${winHeader + winSize}.")
      val average = winRDD.reduce(_ + _) / winSize
      YLogger.ylogInfo(this.getClass.getSimpleName) (s"the average is ${average}")
      val winAve = sc.parallelize(Seq(average))
      YLogger.ylogInfo(this.getClass.getSimpleName) (s"create rdd called winAve[${winAve.id}] which stores average.")
      midRDD = midRDD.union(winAve).persist(StorageLevel.MEMORY_AND_DISK)
      midRDD.count()
      YLogger.ylogInfo(this.getClass.getSimpleName) (s"union winAve[${winAve.id}] and midRDD[${midRDD.id}]."
      )
      winHeader += winStep
      YLogger.ylogInfo(this.getClass.getSimpleName) ("\r\n")
    }
    val nextWinValue = midRDD.reduce(_ + _) / winLength
    YLogger.ylogInfo(this.getClass.getSimpleName) (s"aggregate rdd of windows for average -> ${nextWinValue}")
    sc.stop()
  }
}