package pers.yzq.spark.hbase.MovingAverage
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import pers.yzq.spark.{PropertiesHelper, YLogger}
import pers.yzq.spark.hbase.Common

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

    val conf = new SparkConf()
      .setAppName("TWA-HBASE-MAv1" + System.currentTimeMillis())
    val sc = new SparkContext(conf)
    val common = new Common

    var winHeader = winStart
    var startTimeStamp = winHeader
    var endTimeStamp = startTimeStamp + winSize
    var winRDD = sc.emptyRDD[(Long, Long)]
    var midRDD = sc.emptyRDD[Long].persist(StorageLevel.MEMORY_ONLY)
    for(i <- Range(0, winLength)) {
      // Fetch data from HBase with the restriction of start and end.
      val rdd = common.trans2DT(common.loadRDD(sc,start = startTimeStamp, end = endTimeStamp)).persist(StorageLevel.MEMORY_AND_DISK)
      rdd.count()
      // the new part of winRDD was cached in memory.
      YLogger.ylogInfo(this.getClass.getSimpleName)(s"rdd[${winRDD.id}] fetches data which ranges from ${startTimeStamp} to ${endTimeStamp}.")
      // There is no need to repartition.
      // winRDD is public and needed to be cached partly in memory.
      winRDD = winRDD.filter((a: (Long, Long)) => {
        a._2 >= winHeader
      }).persist(StorageLevel.MEMORY_ONLY)
      winRDD.count()
      // the preceding part of winRDD was handled in memory
      winRDD = winRDD.union(rdd)
      winRDD.count()
      YLogger.ylogInfo(this.getClass.getSimpleName) (s"winRDD unions rdd and itself which ranges from ${winHeader} to ${endTimeStamp}.")
      // Calculate the time window.
      val average = winRDD.map(e => e._1).reduce((a, b) => a + b) / winSize
      YLogger.ylogInfo(this.getClass.getSimpleName) (s"the average is ${average}.")
      val winAve = sc.parallelize(Seq(average))
      YLogger.ylogInfo(this.getClass.getSimpleName)(s"create rdd called winAve[${winAve.id}] which stores the average.")
      midRDD = midRDD.union(winAve).persist(StorageLevel.MEMORY_ONLY)
      midRDD.count()

      startTimeStamp = winHeader + Math.max(winSize, winStep)
      endTimeStamp = startTimeStamp + Math.min(winSize, winStep)
      winHeader += winStep

      YLogger.ylogInfo(this.getClass.getSimpleName) ("\r\n")
    }
    val nextWinValue = midRDD.reduce(_+_) / winLength
    YLogger.ylogInfo(this.getClass.getSimpleName) (s"aggregate rdd of windows for average -> ${nextWinValue}")
    sc.stop()
  }
}
