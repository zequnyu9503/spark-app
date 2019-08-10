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
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

protected[api] sealed class TimeWindowController[T, V](
    val sc: SparkContext,
    val size: Long,
    val step: Long,
    val func: (T, T) => RDD[(T, V)]) {

  private val entries =
    new util.LinkedHashMap[Integer, RDD[(T, V)]](32, 0.75f, true)
  private val winId = new AtomicInteger(0)
  private var partition: Integer = 0
  private val isOverlap: Boolean = if (size > step) true else false
  private val minOne = Math.min(size, step)
  private val maxOne = Math.max(size, step)

  var scope = TimeScope()
  var keepInMem: Integer = 1
  var keepInMemSize = Long.MaxValue
  var storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  var partitionLimitations: Integer = partition

  /**
    *判断下一个RDD是否为空.
    * 如果设置了时间范围, 仅判断RDD是否处于时间范围内.
    * 否则调用RDD.isEmpty计算结果.
    * @return 如果RDD处于时间范围内, RDD也可能为空.
    */
  def isEmpty: Boolean = {
    var isEpt: Boolean = true
    if (scope.isDefault) {
      // 保存当前时间参数.
      TimeWindowController.save()
      update()
      nextRDD() match {
        case Some(rdd) =>
          isEpt = rdd._1.isEmpty()
        case _ =>
      }
      TimeWindowController.reset()
    } else {
      isEpt = !scope.isLegal(TimeWindowController.winStart)
    }
    isEpt
  }

  /**
    * 获取下一个RDD.
    * @return RDD必须满足(T, V)类型.
    */
  def next(): RDD[(T, V)] = {
    update()
//    clean(keepInMem)
    nextRDD(true) match {
      case Some(rdds) =>
        entries.put(winId.getAndIncrement(), rdds._2)
        rdds._1
      case _ => null
    }
  }

  /**
    * 获取下一个时间窗口RDD并更新相关参数.
    * @param cached RDD是否被缓存, 一般用于判断RDD是否为空.
    * @return (RDD0, RDD1) 分别代表窗口RDD与缓存部分RDD.
    */
  private def nextRDD(
      cached: Boolean = false): Option[(RDD[(T, V)], RDD[(T, V)])] = {
    try {
      // suffixRDD 只能从非缓存空间获取.
      val suffixRDD = func(TimeWindowController.startTime.asInstanceOf[T],
                           TimeWindowController.endTime.asInstanceOf[T])
      latestCachedRDD() match {
        case Some(rdd) =>
          // 如果缓存RDD存在则意味着窗口之间存在重叠.
          val prefixRDD = rdd
          val coarseRDD = prefixRDD.++(suffixRDD).
            setName(s"CoarseTimeWindowRDD[${winId.get()}]")
          val border = TimeWindowController.winStart + step
          if (cached) {
            val wasteRDD = coarseRDD.filter(_._1.asInstanceOf[Long] < border)
            val cacheRDD = coarseRDD.filter(_._1.asInstanceOf[Long] >= border).persist(storageLevel)
            var delicateRDD = wasteRDD.++(cacheRDD)
            if (partitions() > 0) delicateRDD = delicateRDD.coalesce(partitions())
            Option(delicateRDD.
              setName(s"DelicateTimeWindowRDD[${winId.get()}]"), cacheRDD)
          } else {
            Option(coarseRDD, rdd)
          }
        case None =>
          if (partition == 0) partition = suffixRDD.getNumPartitions
          if (cached && isOverlap) {
            val border = TimeWindowController.winStart + step
            val wasteRDD = suffixRDD.filter(_._1.asInstanceOf[Long] < border)
            val cacheRDD = suffixRDD.filter(_._1.asInstanceOf[Long] >= border).
              persist(storageLevel)
            var delicateRDD = wasteRDD.union(cacheRDD)
            if (partitions() > 0) delicateRDD = delicateRDD.coalesce(partitions(1))
            Option(delicateRDD.
              setName(s"DelicateTimeWindowRDD[${winId.get()}]"), cacheRDD)
          } else {
            Option(suffixRDD, null)
          }
      }
    } catch {
      case e: Exception =>
        // scalastyle:off println
        System.err.println(e.printStackTrace())
        // scalastyle:on println
        None
    }
  }

  /**
    * 更新当前时间参数.
    */
  private def update(): Unit = {
    if (TimeWindowController.initialized) {
      TimeWindowController.startTime = TimeWindowController.winStart + maxOne
      TimeWindowController.endTime = TimeWindowController.startTime + minOne
      TimeWindowController.winStart += step
    } else {
      TimeWindowController.winStart = scope.start
      TimeWindowController.startTime = TimeWindowController.winStart
      TimeWindowController.endTime = TimeWindowController.winStart + size
      TimeWindowController.initialized = !TimeWindowController.initialized
    }
  }

  private def disable(id: Integer): TimeWindowController[T, V] = {
    if (entries.containsKey(id)) {
      entries.get(id).unpersist(true)
    }
    this
  }

  private def remove(id: Integer): TimeWindowController[T, V] = {
    if (entries.containsKey(id)) {
      entries.remove(id)
    }
    this
  }

  private def clean(keepInMem: Integer): Unit = {
    if (entries.size() > keepInMem) {
      val delList = new util.ArrayList[Integer]
      val delLimit = latestWinId() - (entries.size() - keepInMem)
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

  private def sizeInMem(winId: Integer): Long = {
    sc.getRDDStorageInfo.find(_.id == rddId(winId)) match {
      case Some(info) => info.memSize
      case _ => -1
    }
  }

  private def cachedMem(): Long = {
    var memSize = 0L
    val itr = entries.entrySet().iterator()
    while (itr.hasNext) {
      memSize += sizeInMem(rddId(itr.next().getKey))
    }
    memSize
  }

  private def rddId(winId: Integer): Integer = latestCachedRDD(winId) match {
    case Some(rdd) => rdd.id
    case _ => -1
  }

  private def latestCachedRDD(n: Integer = latestWinId()): Option[RDD[(T, V)]] = {
    if (entries.containsKey(n)) Option(entries.get(n)) else None
  }

  private def latestWinId(): Integer = {
    val nextId = winId.get()
    if (nextId > 0) nextId - 1 else nextId
  }

  private def partitions(times: Integer = 2): Integer = {
    if (partitionLimitations > partition) {
      partitionLimitations
    } else {
      times * partition
    }
  }
}

protected[api] object TimeWindowController {
  var initialized: Boolean = false

  // 重点: 时间窗口RDD控制参数由于涉及创建与转换操作
  // 其变量必须采用静态变量或者序列化对象.
  var winStart: Long = 0L
  var startTime: Long = 0L
  var endTime: Long = 0L

  // 用户保存某次操作的时间参数.
  var record: (Long, Long, Long, Boolean) =
    (winStart, startTime, endTime, initialized)

  /**
    * 保存当前时间参数.
    */
  def save(): Unit =
    record = (winStart, startTime, endTime, initialized)

  /**
    * 恢复当前时间参数.
    */
  def reset(): Unit = {
    winStart = record._1
    startTime = record._2
    endTime = record._3
    initialized = record._4
  }
}
