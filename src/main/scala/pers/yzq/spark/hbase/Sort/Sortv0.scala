package pers.yzq.spark.hbase.Sort

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import pers.yzq.spark.PropertiesHelper
import pers.yzq.spark.hbase.Common.Common

/**
  *
  * @Author: YZQ
  * @date 2019/6/14
  */
object Sortv0 {
  def main(args: Array[String]): Unit = {

    val winStart = PropertiesHelper.getProperty("twa.start").toLong
    @Deprecated
    val winEnd = PropertiesHelper.getProperty("twa.end").toLong
    val winSize = PropertiesHelper.getProperty("twa.win.size").toLong
    val winStep = PropertiesHelper.getProperty("twa.win.step").toLong
    val winLength = PropertiesHelper.getProperty("twa.win.length").toInt

    val conf = new SparkConf().setAppName("TWA-HBASE-Sortv0-" + System.currentTimeMillis())
    val sc = new SparkContext(conf)
    val common = new Common

    var winHeader = winStart
    var midRDD = sc.emptyRDD[(Long, Long)].persist(StorageLevel.MEMORY_AND_DISK)
    for(i <- Range(0, winLength)){
      val rdd = common.trans2DT(common.loadRDD(sc, start = winHeader,end = winHeader+winSize))
      midRDD = midRDD.union(rdd.sortBy(_._1)).persist(StorageLevel.MEMORY_AND_DISK)
      winHeader += winStep
    }
    val sorted = midRDD.sortBy(_._1)
  }
}
