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

package pers.yzq.spark.datasets

import java.io.File
import java.nio.charset.Charset

import com.google.common.io.{Files, LineProcessor}

object LocalHandler {

  def main(args: Array[String]): Unit = {
    val f = new File("/opt/zequnyu/res_1.txt")
    Files.readLines(f, Charset.defaultCharset(), new JobReader[String])
  }

  class JobReader[String] extends LineProcessor[String] {

    var job: java.lang.String = _

    var total: Int = 0

    var nf: File = null

    override def processLine(line: Predef.String): Boolean = {
      val filtered = line.replaceAll("[(|)]]", "")
      val splited = filtered.split(",", -1)

      if (job != splited(0)) {
        print("#")
        job = splited(0)
        nf = new File(s"/opt/zequnyu/res_1/job_$job")
      }

      val record = s"${1},${2},${3},${4},${5},${6},${7},${8},${9},${10},${11},${12}"
      Files.append(record, nf, Charset.defaultCharset())
      true
    }

    override def getResult: Integer = total
  }


}
