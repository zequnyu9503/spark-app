package pers.yzq.spark.hbase.NumberCount

import java.util.concurrent.Executors

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.{BinaryComparator, FamilyFilter, FilterList, QualifierFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CompareOperator, HBaseConfiguration}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Queue

/**
  *
  * @Author: YZQ
  * @date 2019/5/26
  */
object NumberCountV2 {
  private var hcp:String =_
  private var tableName:String =_
  private var columnFamily:String =_
  private var columnQualify:String =_
  private var target_file:String =_
  private var wholeTimeStamp:Long = 0L
  private var winSize:Long = 0L
  private var winStep:Long =0L

  private var sc:SparkContext =_

  @volatile
  private var winHeader = 0L
  @volatile
  private var prefHeader =0L
  @volatile
  private var rddQueue = new Queue[RDD[(Long, Long)]]()

  def createRDD(startTimeStamp:Long, endTimeStamp:Long, isCached:Boolean = false) : RDD[(Long, Long)] = {
    // Transform a RDD to another one.
    def transRDD(rdd:RDD[(ImmutableBytesWritable, Result)], isCached:Boolean) : RDD[(Long, Long)] ={
      val _rdd = rdd.map(e => (Bytes.toLong(e._2.listCells().get(0).getValueArray, e._2.listCells().get(0).getValueOffset), e._2.listCells().get(0).getTimestamp))
      // We need an slight weight action operation to execute this job immediately.
      if(isCached){
        _rdd.cache().count()
      }
      _rdd
    }

    // Load RDD from HBase.
    def loadRDD(startTimeStamp:Long, endTimeStamp:Long) : RDD[(ImmutableBytesWritable, Result)] ={
      val hc = HBaseConfiguration.create();
      hc.addResource(new Path(hcp))
      hc.set(TableInputFormat.INPUT_TABLE, tableName)
      hc.set(
        TableInputFormat.SCAN,
        TableMapReduceUtil.convertScanToString(new Scan()
          .setFilter(new FilterList(
            FilterList.Operator.MUST_PASS_ALL,
            new FamilyFilter(
              CompareOperator.EQUAL,
              new BinaryComparator(Bytes.toBytes(columnFamily))),
            new QualifierFilter(CompareOperator.EQUAL,
              new BinaryComparator(Bytes.toBytes(columnQualify)))
          ))
          .setTimeRange(startTimeStamp, endTimeStamp))
      )
      // Using newAPIHadoopRDD
      sc.newAPIHadoopRDD(hc, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    }
    transRDD(loadRDD(startTimeStamp,endTimeStamp), isCached)
  }

  class prefetcher extends Runnable{
    override def run(): Unit = {
      // It works when 'winHeader' is lower than 'wholeTimeStamp'.
      secondaryJob()
    }

    def secondaryJob():Unit = {
      val startTimeStamp = winHeader + 1 * winStep
      val endTimeStamp = startTimeStamp + winSize
      if(startTimeStamp <= wholeTimeStamp) {
        val rdd = createRDD(startTimeStamp, endTimeStamp, true)
        if(winHeader >= startTimeStamp){
          rdd.unpersist(true)
          println("RDD cached is depecerated")
        }else{
          rddQueue.enqueue(rdd)
        }
        prefHeader += winStep
      }
    }
  }

  def mainJob():Unit = {
    val tpool = Executors.newFixedThreadPool(1)
    while (winHeader <= wholeTimeStamp) {
      tpool.submit(new prefetcher)
      if (rddQueue.isEmpty) {
        println("\nThere is no RDD prefetched before. Start to load RDD.\n")
        val rdd = createRDD(winHeader, winHeader + winSize, false)
        rddQueue.enqueue(rdd)
      }
      val _rdd = rddQueue.dequeue()
      _rdd.map(e => (e._1, 1L)).reduceByKey((a, b) => a + b).saveAsTextFile(target_file + "-" + System.currentTimeMillis())
      winHeader += winStep
    }
    tpool.shutdown()
  }

  /**
    * Assume time-increasing windows
    * @param args
    */
  def main(args: Array[String]): Unit = {
    // hbase-site.xml
    hcp = args(0)
    // Table Name
    tableName = args(1)
    //Column Family
    columnFamily = args(2)
    // Column
    columnQualify = args(3)
    //Hadoop target file
    target_file = args(4)
    // the whole time stamp
    wholeTimeStamp = args(5).toLong
    // time window size
    winSize = args(6).toLong
    // time window step
    winStep = args(7).toLong

    val conf = new SparkConf().
      setAppName("YZQ-TWA-NUMBERCOUNT-HBASE-" + System.currentTimeMillis()).
      set("spark.scheduler.mode", "FAIR")
    sc = new SparkContext(conf)
    sc.setCheckpointDir("/checkpoints")

    mainJob()
    //app-20190527103321-0087
  }
}
