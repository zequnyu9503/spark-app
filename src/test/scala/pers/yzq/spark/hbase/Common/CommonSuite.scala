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

import java.util

import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.FunSuite

class CommonSuite extends FunSuite{

  def split(): Array[Array[Byte]] = {
    val splitSet = new Array[Array[Byte]](9)
    val set = new util.TreeSet[Array[Byte]](Bytes.BYTES_COMPARATOR)
    for (i <- Range(0, 9))
      set.add(Bytes.toBytes((98 + i).asInstanceOf[Char] + "0000000000"))
    val itr = set.iterator
    var index: Integer = 0
    while (itr.hasNext) {
      splitSet(index) = itr.next()
      index += 1
    }
    splitSet
  }

  test("splits") {
    val s = split()
    // scalastyle:off println
    println(s.eq(null))
  }

  test("drop table") {
    // scalastyle:off println
    val tableName = "Exam_for_Creation"
    if (HBaseCommon.dropDeleteTable(tableName)) {
      println("删除成功")
    }
  }

  test("create table") {
    val tableName = "Exam_for_Creation"
    val columnFamilies = Array("CF1", "CF2", "CF3")
    val splits = split()
    // scalastyle:off println
    if (HBaseCommon.createTable(tableName, columnFamilies, splits).equals(true)) {
      println("创建成功")
    }
  }
}
