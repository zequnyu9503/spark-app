
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
package pers.yzq.spark
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.FunSuite

/**
  *
  * @Author: YZQ
  * @date 2019/6/5
  */
class Scala extends FunSuite {

  test("String") {
    val num: Long = 21
    val res = String.format("%07d", java.lang.Long.valueOf(num))
    println(res)

    for (i <- Range(0, 9))
      println((98 + i).asInstanceOf[Char] + "0000000000")

    println((0 % 10 + 97).asInstanceOf[Char])
  }
}
