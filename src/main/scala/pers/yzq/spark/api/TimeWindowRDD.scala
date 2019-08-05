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

package pers.yzq.spark.api

import org.apache.spark.rdd.RDD

sealed class TimeWindowRDD[T, V](winSize: T,
                                 winStep: T,
                                 func: (T, T) => RDD[(T, V)]) {

  private val controller = new TimeWindowController(winSize.asInstanceOf[Long],
                                                    winStep.asInstanceOf[Long],
                                                    func)

  private var _iterator: TimeWindowIterator[T, V] = _

  def iterator(): TimeWindowIterator[T, V] = {
    if (_iterator.eq(null)) _iterator = new TimeWindowIterator[T, V](this)
    _iterator
  }

  def setScope(start: T, end: T): TimeWindowRDD[T, V] = {
    controller.scope =
      TimeScope(start.asInstanceOf[Long], end.asInstanceOf[Long])
    this
  }

  def setKeepInMem(n: Integer): TimeWindowRDD[T, V] = {
    if (n > 0) controller.keepInMem = n
    this
  }

  def setPartitionsLimitations(n: Integer): TimeWindowRDD[T, V] = {
    if (n > 0) controller.partitionLimitations = n
    this
  }

  protected[api] def nextWinRDD(): RDD[(T, V)] = {
    controller.next()
  }

  protected[api] def isNextEmpty: Boolean = {
    controller.isEmpty
  }
}
