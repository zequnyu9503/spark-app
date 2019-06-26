package pers.yzq.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

/**
  *
  * @Author: YZQ
  * @date 2019/6/5
  */
class Scala extends FunSuite {

  test("runJob") {
    val conf = new SparkConf().setMaster("local").setAppName("runJob")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")
    val rdd = sc.textFile("E:/Project/Top Project/run.txt", 2)
    rdd.map(e => "e")
    val count = rdd.count()
    println(s"${count} lines in run.sh.")
    println(s"${rdd.getNumPartitions} partitions")
  }
}
