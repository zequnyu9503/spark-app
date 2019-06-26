package pers.yzq.spark.hbase.MovingAverage

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import pers.yzq.spark.PropertiesHelper
import pers.yzq.spark.hbase.Common

import scala.collection.mutable.Queue

/**
  *
  * @Author: YZQ
  * @date 2019/6/3
  */
object MAv2 extends{

  @volatile
  private var winHeader = 0L
  @volatile
  private var rddQueue = new Queue[RDD[(Long, Long)]]()

  class prefetcher(sc:SparkContext, pStart:Long, pEnd:Long) extends Runnable{

    val common = new Common

    override def run(): Unit = {
      val rdd = common.trans2DT(common.loadRDD(sc,start = pStart, end = pEnd).persist(StorageLevel.MEMORY_ONLY))
      rdd.count()
      if(winHeader >= pStart){
        rdd.unpersist(true)
      }else{
        rddQueue.enqueue(rdd)
      }
    }
  }

  def main(args: Array[String]): Unit = {

    val winStart = PropertiesHelper.getProperty("twa.start").toLong
    @Deprecated
    val winEnd = PropertiesHelper.getProperty("twa.end").toLong
    val winSize = PropertiesHelper.getProperty("twa.win.size").toLong
    val winStep = PropertiesHelper.getProperty("twa.win.step").toLong
    val winLength = PropertiesHelper.getProperty("twa.win.length").toInt

    val conf = new SparkConf()
      .setAppName("TWA-HBASE-MAv2" + System.currentTimeMillis())
    val sc = new SparkContext(conf)
    val common = new Common

    winHeader = winStart
    var startTimeStamp = winHeader
    var endTimeStamp = startTimeStamp + winSize
    var winRDD = sc.emptyRDD[(Long, Long)]
    var midRDD = sc.emptyRDD[Long].persist(StorageLevel.MEMORY_ONLY)
    for(i <- Range(0, winLength)) {
      if(rddQueue.isEmpty){
        // Fetch data from HBase with the restriction of start and end.
        // rdd is a fraction of the next winRDD.
        var rdd = common.trans2DT(common.loadRDD(sc,start = startTimeStamp, end = endTimeStamp))
        rdd = rdd.union(winRDD.filter(e => e._2 >= winHeader)).persist(StorageLevel.MEMORY_ONLY)
        rddQueue.enqueue(rdd)
      }
      winRDD = rddQueue.dequeue()
      // Calculate the time window.
      val winAve = winRDD.map(e => e._1).reduce((a, b) => a + b) / winSize

      midRDD = midRDD.union(sc.parallelize(Seq(winAve))).persist(StorageLevel.MEMORY_ONLY)


      startTimeStamp = winHeader + Math.max(winSize, winStep)
      endTimeStamp = startTimeStamp + Math.min(winSize, winStep)
      winHeader += winStep
    }
    val nextWinValue = midRDD.reduce(_+_) / winLength
  }
}
