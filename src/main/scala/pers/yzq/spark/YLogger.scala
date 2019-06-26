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

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.logging._

import scala.collection.mutable

protected [spark] object YLogger extends Serializable {

  private var ylogs_ = new mutable.HashMap[String, Logger]()

  private def initializeYLogging(className: String): Unit = {
    val logPath = PropertiesHelper.getProperty("logging.path") + File.separator + className + ".log"
    val log_ = Logger.getLogger(className)
    val fileHandler = new FileHandler(logPath, false)
    val formatter = new Formatter {
      override def format(record: LogRecord): String = {
        val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
        new StringBuilder()
          .append("[")
          .append(className)
          .append("]")
          .append("[")
          .append(time)
          .append("]")
          .append(":: ")
          .append(record.getMessage)
          .append("\r\n")
          .toString
      }
    }
    fileHandler.setFormatter(formatter)
    log_.addHandler(fileHandler)
    ylogs_.put(className, log_)
  }

  private def ylog(logName: String): Logger = {
    if (!ylogs_.contains(logName)) {
      initializeYLogging(logName)
    }
    ylogs_.get(logName).get
  }

  def ylogInfo(logName: String)(info: String): Unit = {
    if (ylog(logName).isLoggable(Level.INFO)) ylog(logName).info(info)
  }

  def ylogWarning(logName: String)(warning: String): Unit = {
    if (ylog(logName).isLoggable(Level.WARNING)) ylog(logName).warning(warning)
  }
}
