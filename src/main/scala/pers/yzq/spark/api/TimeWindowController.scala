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

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

protected[api] sealed class TimeWindowController[T, V](
    val size: Long,
    val step: Long,
    val func: (T, T) => RDD[(T, V)]) {

  private val entries =
    new util.LinkedHashMap[Integer, RDD[(T, V)]](32, 0.75f, true)

  private val winId = new AtomicInteger(0)

  var scope = TimeScope()

  var keepInMem: Integer = 1

  private var partition: Integer = 0

  var partitionLimitations: Integer = partition

  def isEmpty: Boolean = {
    if (scope.isDefault) {
      nextRDD() match {
        case Some(rdd) => rdd.isEmpty()
        case _ => true
      }
    } else {
      !scope.isLegal(TimeWindowController.winStart)
    }
  }

  private def nextRDD(cached: Boolean = false): Option[RDD[(T, V)]] =
    try {
      var suffixRDD = func(TimeWindowController.startTime.asInstanceOf[T],
                           TimeWindowController.endTime.asInstanceOf[T])
      latest() match {
        case Some(rdd) =>
          var prefixRDD =
            rdd.filter(_._1.asInstanceOf[Long] >= TimeWindowController.winStart)
          if (partitions() > 0) prefixRDD = prefixRDD.coalesce(partitions())
          var next = prefixRDD.union(suffixRDD)
          if (cached) {
            next = next
              .persist(StorageLevel.MEMORY_ONLY)
              .setName(s"TimeWindowRDD[${winId.get()}]")
          }
          Option(next)
        case None =>
          if (partition == 0) partition = suffixRDD.getNumPartitions
          if (cached) {
            suffixRDD = suffixRDD
              .persist(StorageLevel.MEMORY_ONLY)
              .setName(s"TimeWindowRDD[${winId.get()}]")
          }
          Option(suffixRDD)
      }
    } catch {
      case e: Exception =>
        None
    }

  def next(): RDD[(T, V)] = {
    update()
    clean(keepInMem)
    nextRDD(true) match {
      case Some(rdd) =>
        entries.put(winId.getAndIncrement(), rdd)
        rdd
      case _ => null
    }
  }

  def update(): Unit = synchronized {
    if (TimeWindowController.initialized) {
      TimeWindowController.startTime = TimeWindowController.winStart + Math
        .max(size, step)
      TimeWindowController.endTime = TimeWindowController.startTime + Math
        .min(size, step)
      TimeWindowController.winStart += step
    } else {
      TimeWindowController.winStart = scope.start
      TimeWindowController.startTime = TimeWindowController.winStart
      TimeWindowController.endTime = TimeWindowController.startTime + size
      TimeWindowController.initialized = !TimeWindowController.initialized
    }
  }

  def disable(id: Integer): TimeWindowController[T, V] = {
    if (entries.containsKey(id)) {
      entries.get(id).unpersist(true)
    }
    this
  }

  def remove(id: Integer): TimeWindowController[T, V] = {
    if (entries.containsKey(id)) {
      entries.remove(id)
    }
    this
  }

  def clean(keepInMem: Integer): Unit = {
    if (entries.size() > keepInMem) {
      val delList = new util.ArrayList[Integer]
      val delLimit = lastWinId() - (entries.size() - keepInMem)
      val itr = entries.entrySet().iterator()
      while (itr.hasNext) {
        val rddId = itr.next().getKey
        if (rddId <= delLimit) delList.add(rddId)
      }
      val itr_ = delList.iterator()
      while (itr_.hasNext) {
        val rddId = itr_.next()
        this.disable(rddId).remove(rddId)
      }
    }
  }

  def latest(n: Integer = lastWinId()): Option[RDD[(T, V)]] =
    if (entries.containsKey(n)) {
      Option(entries.get(n))
    } else {
      None
    }

  def lastWinId(): Integer = {
    val nextId = winId.get()
    if (nextId > 0) {
      nextId - 1
    } else {
      nextId
    }
  }

  def partitions(): Integer = {
    val differential = partitionLimitations - partition
    if (differential > partition) {
      differential
    } else {
      partition
    }
  }
}

protected[api] object TimeWindowController {
  private var initialized: Boolean = false

  // 重点: 时间窗口RDD控制参数由于涉及创建与转换操作
  // 其变量必须采用静态变量或者序列化对象.
  var winStart: Long = 0L
  var startTime: Long = 0L
  var endTime: Long = 0L
}
