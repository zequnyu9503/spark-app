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

import org.apache.spark.Partitioner

class JobPartitioner extends Partitioner {

  private val minKey = 3418309L
  private val maxKey = 6486641236L
  private val total = 672004L

  override def numPartitions: Int = {
    // 5个计算节点, 每个节点24个核心, 每个核心处理2个分区.
    5 * 24 * 2
  }

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[String].toLong
    ((k - minKey) / (maxKey - minKey) * (numPartitions - 1)).toInt
  }
}
