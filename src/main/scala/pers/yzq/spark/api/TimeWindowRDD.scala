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

import java.util
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

sealed class TimeWindowRDD[T, V](sc: SparkContext,
                                 val winSize: T,
                                 val winStep: T,
                                 val func: (T, T) => RDD[(T, V)])
    extends Logging {

  private val entries =
    new util.LinkedHashMap[Integer, RDD[(T, V)]](32, 0.75f, true)
  private val controller = new TimeWindowController(winSize.asInstanceOf[Long],
                                                    winStep.asInstanceOf[Long],
                                                    entries)
  private var _iterator: TimeWindowIterator[T, V] = _

  def iterator(): TimeWindowIterator[T, V] = {
    if (_iterator.eq(null)) _iterator = new TimeWindowIterator[T, V](this)
    _iterator
  }

  def setScope(start: T, end: T): TimeWindowRDD[T, V] = {
    controller.scope = TimeScope(start.asInstanceOf[Long], end.asInstanceOf[Long])
    this
  }

  def setKeepInMem(n: Integer): TimeWindowRDD[T, V] = {
    if (n > 0) controller.keepInMem = n
    this
  }

  protected[api] def nextWinRDD(): RDD[(T, V)] = {
    controller.next()
  }

  protected[api] def isNextEmpty: Boolean = {
    controller.isEmpty
  }

  private[TimeWindowRDD] class TimeWindowController(
      val size: Long,
      val step: Long,
      val entries: util.LinkedHashMap[Integer, RDD[(T, V)]]) {

    private val winId = new AtomicInteger(0)

    var scope = TimeScope()

    var keepInMem: Integer = 1

    private var winStart: Long = 0L
    private var startTime: Long = winStart
    private var endTime: Long = winStart + size

    def isEmpty: Boolean = {
      if (scope.isDefault) {
        nextRDD().isEmpty
      } else {
        scope.isLegal(winStart)
      }
    }

    private def nextRDD(): Option[RDD[(T, V)]] =
      try {
        val prefixRDD = latest().filter(_._1.asInstanceOf[Long] >= winStart)
        val suffixRDD = func(startTime.asInstanceOf[T], endTime.asInstanceOf[T])
        val RDD =
          suffixRDD.union(prefixRDD).setName(s"TimeWindowRDD[${winId.get()}]")
        Option(RDD)
      } catch {
        case e: Exception => None
      }

    def next(): RDD[(T, V)] = {
      nextRDD() match {
        case Some(rdd) =>
          entries.put(winId.getAndIncrement(), rdd)
          update()
          clean(keepInMem)
          rdd
        case None => null
      }
    }

    def update(): Unit = synchronized {
      startTime = winStart + Math.max(size, step)
      endTime = startTime + Math.min(size, step)
      winStart += step
    }

    def disable(id: Integer): Unit = {
      if (id < entries.size() && entries.containsKey(id)) {
        entries.get(id).unpersist(true)
      }
    }

    def remove(id: Integer): Unit =
      if (id < entries.size() && entries.containsKey(id)) entries.remove(id)

    def clean(keepInMem: Integer): Unit = {
      if (entries.size() > keepInMem) {
        val range = entries.size() - keepInMem
        val iterator = entries.entrySet().iterator()
        for (i <- Range(0, range) if iterator.hasNext) {
          val entry = iterator.next()
          val id = entry.getKey
          disable(id)
          remove(id)
        }
      }
    }

    def latest(n: Integer = lastWinId()): RDD[(T, V)] =
      if (entries.containsKey(n)) entries.get(n) else sc.emptyRDD[(T, V)]

    def lastWinId(): Integer = {
      val nextId = winId.get()
      if (nextId > 0) nextId - 1 else nextId
    }
  }

  private[TimeWindowRDD] case class TimeScope(start: Long = 0L,
                                              end: Long = Long.MaxValue) {

    def isDefault: Boolean = end == Long.MaxValue

    def isLegal(winStart: Long): Boolean =
      winStart >= start.asInstanceOf[Long] && winStart <= end.asInstanceOf[Long]
  }
}
