package pers.yzq.spark.hbase

import pers.yzq.spark.{HBaseBulkLoad, PropertiesHelper, YLogger}

object BulkLoad {

  def main(args: Array[String]): Unit = {
    // Table Name
    val tableName = PropertiesHelper.getProperty("hbase.bulkload.tablename")
    // Column Family
    val family = PropertiesHelper.getProperty("hbase.bulkload.columnfamily")
    // Column
    val column = PropertiesHelper.getProperty("hbase.bulkload.columnqualify")
    // Hadoop File
    val hadoop_file = PropertiesHelper.getProperty("hbase.bulkload.hadoopfile")
    // HFile
    val hfile = PropertiesHelper.getProperty("hbase.bulkload.hfile")
    YLogger.ylogInfo(this.getClass.getSimpleName) (s"BulkLoad config: tableName[${tableName}] " +
      s"family[${family}] column[${column}] hadoop_file[${hadoop_file}] hfile[${hfile}].")

    val bulkLoad = new HBaseBulkLoad
    bulkLoad.bulkLoad(tableName, family, column, hadoop_file, hfile)
  }
}
