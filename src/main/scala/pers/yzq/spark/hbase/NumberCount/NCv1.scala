package pers.yzq.spark.hbase.NumberCount

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import pers.yzq.spark.PropertiesHelper
import pers.yzq.spark.hbase.Common.Common

/**
  *
  * @Author: YZQ
  * @date 2019/6/15
  */
object NCv1 {

  val winStart = PropertiesHelper.getProperty("twa.start").toLong
  @Deprecated
  val winEnd = PropertiesHelper.getProperty("twa.end").toLong
  val winSize = PropertiesHelper.getProperty("twa.win.size").toLong
  val winStep = PropertiesHelper.getProperty("twa.win.step").toLong
  val winLength = PropertiesHelper.getProperty("twa.win.length").toInt

  val conf = new SparkConf().setAppName("TWA-HBASE-NumberCountV0-" + System.currentTimeMillis())
  val sc = new SparkContext(conf)
  val common = new Common

  var winHeader = winStart
  var midRDD = sc.emptyRDD[(Long, Long)].persist(StorageLevel.MEMORY_AND_DISK)
  for(i <- Range(0, winLength)){
    val rdd = common.trans2D(common.loadRDD(sc, start = winHeader,end = winHeader+winSize))
    val winSta = rdd.map(e => (e, 1L)).reduceByKey(_+_).filter(_._2 > 1)
    midRDD = midRDD.union(winSta).persist(StorageLevel.MEMORY_AND_DISK)
    winHeader += winStep
  }
  val resSta = midRDD.map(e => (e, 1L)).reduceByKey(_+_)
}
