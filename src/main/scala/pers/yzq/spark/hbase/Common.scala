package pers.yzq.spark.hbase

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{CompareOperator, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.{BinaryComparator, FamilyFilter, FilterList, QualifierFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import pers.yzq.spark.PropertiesHelper

/**
  *
  * @Author: YZQ
  * @date 2019/6/3
  */
class Common {
  def loadRDD(sc:SparkContext,
              tableName:String = PropertiesHelper.getProperty("hbase.tablename"),
              columnFamily:String = PropertiesHelper.getProperty("hbase.columnfamily"),
              columnQualify:String = PropertiesHelper.getProperty("hbase.columnqualify"),
              start:Long = PropertiesHelper.getProperty("twa.start").toLong,
              end:Long = PropertiesHelper.getProperty("twa.end").toLong) : RDD[(ImmutableBytesWritable, Result)] ={
    val hcp = PropertiesHelper.getProperty("hbase.hcp")
    val hc = HBaseConfiguration.create()
    hc.addResource(new Path(hcp))
    hc.set(TableInputFormat.INPUT_TABLE, tableName)
    hc.set(TableInputFormat.SCAN,
      TableMapReduceUtil.convertScanToString(
        new Scan().setFilter(
          new FilterList(
            FilterList.Operator.MUST_PASS_ALL,
            new FamilyFilter(
              CompareOperator.EQUAL,
              new BinaryComparator(Bytes.toBytes(columnFamily))),
            new QualifierFilter(CompareOperator.EQUAL,
              new BinaryComparator(Bytes.toBytes(columnQualify)))
          ))
          .setTimeRange(start, end))
    )
    sc.newAPIHadoopRDD(hc, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
  }

  def trans2DT(rdd: RDD[(ImmutableBytesWritable, Result)]) =
    rdd.map(
      e => (Bytes.toLong(e._2.listCells().get(0).getValueArray), e._2.listCells().get(0).getTimestamp))

  def trans2D(rdd: RDD[(ImmutableBytesWritable, Result)]) =
    rdd.map(e => Bytes.toLong(e._2.listCells().get(0).getValueArray))
}
