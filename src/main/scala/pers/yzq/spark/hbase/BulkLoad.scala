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

package pers.yzq.spark.hbase

import pers.yzq.spark.{HBaseBulkLoad, PropertiesHelper, YLogger}

object BulkLoad extends Serializable {

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
