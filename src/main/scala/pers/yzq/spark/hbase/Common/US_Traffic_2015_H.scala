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

import java.io.{BufferedReader, File, FileReader}
import java.util

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes

object US_Traffic_2015_H {

  def main(args: Array[String]): Unit = {
    val path = "/home/zc/Documents/downloads/dot_traffic_2015.txt"
    val tableName = "US_Traffic"
    val columnFamily = "dot_traffic_2015"

    HBaseCommon.dropDeleteTable(tableName)
    HBaseCommon.createTable(tableName, Array(columnFamily), split())

    val file = new File(path)
    val fr = new FileReader(file)
    val reader = new BufferedReader(fr, 1024 * 1024 * 512)
    var record_id: Long = 0L
    val first = reader.readLine()
    val conf = HBaseConfiguration.create
    val conn = ConnectionFactory.createConnection(conf)
    val table = conn.getTable(TableName.valueOf(tableName)).asInstanceOf[HTable]

    while (first != null) {
      val line = reader.readLine()
      val newLines = line.replaceAll("\"", "").split(",")
      val rowKey = {
        val prefix = (record_id % 10 + 97).asInstanceOf[Char]
        new StringBuilder(prefix).append(record_id).toString()
      }
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("date"),
                    Bytes.toBytes(newLines(0)))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("day_of_data"),
                    Bytes.toBytes(newLines(1).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("day_of_week"),
                    Bytes.toBytes(newLines(2).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("direction_of_travel"),
                    Bytes.toBytes(newLines(3)))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("direction_of_travel_name"),
                    Bytes.toBytes(newLines(4)))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("fips_state_code"),
                    Bytes.toBytes(newLines(5)))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("functional_classification"),
                    Bytes.toBytes(newLines(6).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("functional_classification_name"),
                    Bytes.toBytes(newLines(7).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("lane_of_travel"),
                    Bytes.toBytes(newLines(8).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("month_of_data"),
                    Bytes.toBytes(newLines(8).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("station_id"),
                    Bytes.toBytes(newLines(10).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("restrictions"),
                    Bytes.toBytes(newLines(11)))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("station_id"),
                    Bytes.toBytes(newLines(12).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("traffic_volume_counted_after_0000_to_0100"),
                    Bytes.toBytes(newLines(13).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("traffic_volume_counted_after_0100_to_0200"),
                    Bytes.toBytes(newLines(14).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("traffic_volume_counted_after_0200_to_0300"),
                    Bytes.toBytes(newLines(15).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("traffic_volume_counted_after_0300_to_0400"),
                    Bytes.toBytes(newLines(16).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("traffic_volume_counted_after_0400_to_0500"),
                    Bytes.toBytes(newLines(17).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("traffic_volume_counted_after_0500_to_0600"),
                    Bytes.toBytes(newLines(18).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("traffic_volume_counted_after_0600_to_0700"),
                    Bytes.toBytes(newLines(19).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("traffic_volume_counted_after_0700_to_0800"),
                    Bytes.toBytes(newLines(20).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("traffic_volume_counted_after_0800_to_0900"),
                    Bytes.toBytes(newLines(21).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("traffic_volume_counted_after_0900_to_1000"),
                    Bytes.toBytes(newLines(22).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("traffic_volume_counted_after_1000_to_1100"),
                    Bytes.toBytes(newLines(23).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("traffic_volume_counted_after_1100_to_1200"),
                    Bytes.toBytes(newLines(24).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("traffic_volume_counted_after_1200_to_1300"),
                    Bytes.toBytes(newLines(25).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("traffic_volume_counted_after_1300_to_1400"),
                    Bytes.toBytes(newLines(26).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("traffic_volume_counted_after_1400_to_1500"),
                    Bytes.toBytes(newLines(27).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("traffic_volume_counted_after_1500_to_1600"),
                    Bytes.toBytes(newLines(28).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("traffic_volume_counted_after_1600_to_1700"),
                    Bytes.toBytes(newLines(29).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("traffic_volume_counted_after_1700_to_1800"),
                    Bytes.toBytes(newLines(30).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("traffic_volume_counted_after_1800_to_1900"),
                    Bytes.toBytes(newLines(31).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("traffic_volume_counted_after_1900_to_2000"),
                    Bytes.toBytes(newLines(32).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("traffic_volume_counted_after_2000_to_2100"),
                    Bytes.toBytes(newLines(33).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("traffic_volume_counted_after_2100_to_2200"),
                    Bytes.toBytes(newLines(34).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("traffic_volume_counted_after_2200_to_2300"),
                    Bytes.toBytes(newLines(35).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("traffic_volume_counted_after_2300_to_2400"),
                    Bytes.toBytes(newLines(36).toLong))
      put.addColumn(Bytes.toBytes(columnFamily),
                    Bytes.toBytes("year_of_data"),
                    Bytes.toBytes(newLines(37)))

      table.put(put)
      // scalastyle:off println
      System.err.println(s"Record[${record_id}] inserted into HBase")
      // scalastyle:on println
      record_id += 1
    }

  }

  def split(): Array[Array[Byte]] = {
    val splitSet = new Array[Array[Byte]](9)
    val set = new util.TreeSet[Array[Byte]](Bytes.BYTES_COMPARATOR)
    for (i <- Range(0, 9))
      set.add(Bytes.toBytes((98 + i).asInstanceOf[Char] + "0000000000"))
    val itr = set.iterator
    while (itr.hasNext) splitSet :+ itr.next()
    splitSet
  }
}
