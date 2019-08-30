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

import java.io.{File, FileInputStream}
import java.util.Properties

/**
  *
  * @Author: YZQ
  * @date 2019/5/31
  */
private [spark] object PropertiesHelper extends Serializable {
  private var property : Properties = null

  {
    val twa_path = System.getProperty("user.dir") + File.separator + "twa.properties"
    val twa_file = new File(twa_path)
    if(twa_file.exists()) {
      val stream = new FileInputStream(twa_file)
      property = new Properties()
      property.load(stream)
    } else {
      //scalastyle:off println
      System.err.println("No twa.properties exists below the path of user.dir")
      //scalastyle:on println
      System.exit(-1)
    }
  }

  def getProperty(key:String) : String = property.getProperty(key)
}
