/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pers.yzq.spark.hbase.bulkload

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import pers.yzq.spark.hbase.Common.HBaseCommon

trait BulkLoad {

  var tableName: String = _
  var columnFamily: String = _
  var columnQualify: String = _
  var hfile: String = _
  var hadoop_file: String = _

  def prepare(checkHTable: Boolean): Unit = {
    assert(HBaseCommon.cleanHFiles.equals(true))
    if (checkHTable) {
      assert(HBaseCommon.dropDeleteTable(tableName).equals(true))
      assert(HBaseCommon.createTable(tableName, Array(columnFamily), split()).equals(true))
    }
  }

  def bulkLoad(): Unit = {
    prepare(false)
    val hc = HBaseConfiguration.create
    hc.set("hbase.mapred.outputtable", tableName)
    hc.setLong("hbase.hregion.max.filesize", HConstants.DEFAULT_MAX_FILE_SIZE)
    hc.set("hbase.mapreduce.hfileoutputformat.table.name", tableName)
    hc.setInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY,
      1024 * 1024 * 1024)
    val con = ConnectionFactory.createConnection(hc)
    val admin = con.getAdmin
    val table = con.getTable(TableName.valueOf(tableName))
    val td = table.getDescriptor
    val job = Job.getInstance(hc)
    val rdd_ = rdd()
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job, td)
    rdd_.saveAsNewAPIHadoopFile(hfile,
      classOf[ImmutableBytesWritable], classOf[KeyValue],
      classOf[HFileOutputFormat2], hc)
    val bulkLoader = new LoadIncrementalHFiles(hc)
    val locator = con.getRegionLocator(TableName.valueOf(tableName))
    bulkLoader.doBulkLoad(new Path(hfile), admin, table, locator)
  }

  def rdd(): RDD[(ImmutableBytesWritable, KeyValue)]

  def split(): Array[Array[Byte]]
}