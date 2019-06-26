package pers.yzq.spark.hbase.NumberCount

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import pers.yzq.spark.hbase.Common
import pers.yzq.spark.PropertiesHelper

/**
  *
  * @Author: YZQ
  * @date 2019/5/26
  */
object NCv0 {

  def main(args: Array[String]): Unit = {

    val winStart = PropertiesHelper.getProperty("twa.start").toLong
    @Deprecated
    val winEnd = PropertiesHelper.getProperty("twa.end").toLong
    val winSize = PropertiesHelper.getProperty("twa.win.size").toLong
    val winStep = PropertiesHelper.getProperty("twa.win.step").toLong
    val winLength = PropertiesHelper.getProperty("twa.win.length").toInt
    val savePath = PropertiesHelper.getProperty("save.root.path") + "NCv0-" + System.currentTimeMillis()

    val conf = new SparkConf().setAppName("TWA-HBASE-NCv0-" + System.currentTimeMillis())
    val sc = new SparkContext(conf)
    val common = new Common

    var winHeader = winStart
    var midRDD = sc.emptyRDD[(Long, Long)].persist(StorageLevel.MEMORY_AND_DISK)
    for(i <- Range(0, winLength)){
      val rdd = common.trans2D(common.loadRDD(sc, start = winHeader,end = winHeader+winSize))
      val winSta = rdd.map(e => (e, 1L)).reduceByKey(_+_).filter(_._2 > 1)
      midRDD = winSta.union(midRDD).persist(StorageLevel.MEMORY_AND_DISK)
      winHeader += winStep
    }
    val resSta = midRDD.map(e => (e, 1L)).reduceByKey(_+_)
    resSta.saveAsTextFile(savePath)
  }
}
