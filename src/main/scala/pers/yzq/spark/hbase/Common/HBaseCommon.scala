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

package pers.yzq.spark.hbase.Common

import java.io.IOException
import java.net.{URI, URISyntaxException}
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Admin, ColumnFamilyDescriptor, ColumnFamilyDescriptorBuilder, Connection, ConnectionFactory, TableDescriptorBuilder}
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
import pers.yzq.spark.PropertiesHelper

object HBaseCommon {

  val hcp = PropertiesHelper.getProperty("hbase.hcp")
  val path = PropertiesHelper.getProperty("hdfs.home")
  val user = PropertiesHelper.getProperty("hdfs.user")
  val hfiles = PropertiesHelper.getProperty("hbase.bulkload.hfile")

  def createTable(tableName: String,
                  families: Array[String],
                  splits: Array[Array[Byte]]): Boolean = {
    try {
      val hBaseConfiguration = HBaseConfiguration.create
      hBaseConfiguration.addResource(hcp)
      val connection = ConnectionFactory.createConnection(hBaseConfiguration)
      val admin = connection.getAdmin
      val tn = TableName.valueOf(tableName)
      val tdb = TableDescriptorBuilder.newBuilder(tn)
      val cfdbs = new util.HashSet[ColumnFamilyDescriptor](families.size)
      val familyIterator = families.iterator
      while ({
        familyIterator.hasNext
      }) {
        val family = familyIterator.next
        val cfdb = ColumnFamilyDescriptorBuilder
          .newBuilder(Bytes.toBytes(family))
          .setBlockCacheEnabled(true)
          .setBloomFilterType(BloomType.NONE)
          .setDataBlockEncoding(DataBlockEncoding.NONE)
        cfdbs.add(cfdb.build)
      }
      admin.createTable(tdb.setColumnFamilies(cfdbs).build, splits)
      admin.close()
      true
    } catch {
      case e: IOException =>
        e.printStackTrace()
        false
    }
  }

  def dropDeleteTable(tableName: String): Boolean = {
    try {
      val hBaseConfiguration = HBaseConfiguration.create
      hBaseConfiguration.addResource(hcp)
      val connection = ConnectionFactory.createConnection(hBaseConfiguration)
      val admin = connection.getAdmin
      val tn = TableName.valueOf(tableName)
      if (!admin.isTableDisabled(tn)) admin.disableTable(tn)
      admin.deleteTable(tn)
      admin.close()
      true
    } catch {
      case e: IOException => e.printStackTrace()
        false
    }
  }

  def cleanHFiles: Boolean = {
    try {
      val configuration = new Configuration
      val fileSystem = FileSystem.get(new URI(path), configuration, user)
      return fileSystem.delete(new Path(hfiles), true)
    } catch {
      case e: IOException => e.printStackTrace()
      case e: URISyntaxException => e.printStackTrace()
      case e: InterruptedException => e.printStackTrace()
    }
    false
  }
}
