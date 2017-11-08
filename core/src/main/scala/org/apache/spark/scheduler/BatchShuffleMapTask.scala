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

package org.apache.spark.scheduler

import java.io._
import java.nio.ByteBuffer
import java.util.Properties

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.BlockManagerId

private[spark] class BatchShuffleMapTask(
    stageId: Int,
    stageAttemptId: Int,
    taskBinaries: Broadcast[Array[Byte]],
    partitions: Array[Partition],
    partitionId: Int,
    @transient private var locs: Seq[TaskLocation],
    internalAccumulatorsSer: Array[Byte],
    localProperties: Properties,
    isFutureTask: Boolean,
    nextStageLocs: Option[Seq[BlockManagerId]] = None,
    depShuffleIds: Option[Seq[Seq[Int]]] = None,
    depShuffleNumMaps: Option[Seq[Int]] = None,
    jobId: Option[Int] = None,
    appId: Option[String] = None,
    appAttemptId: Option[String] = None)
  extends Task[Array[MapStatus]](stageId, stageAttemptId, partitionId,
    internalAccumulatorsSer, localProperties, isFutureTask, depShuffleIds, depShuffleNumMaps,
    jobId, appId, appAttemptId)
  with BatchTask
  with Logging {

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  var rdds: Array[RDD[_]] = null
  var deps: Array[ShuffleDependency[_, _, _]] = null

  override def prepTask(): Unit = {
    // Deserialize the RDD using the broadcast variable.
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rddI, depI) = ser.deserialize[(Array[RDD[_]], Array[ShuffleDependency[_, _, _]])](
      ByteBuffer.wrap(taskBinaries.value), Thread.currentThread.getContextClassLoader)
    rdds = rddI
    deps = depI
  }

  def getTasks(): Seq[Task[Any]] = {
    if (deps == null || rdds == null) {
      prepTask()
    }

    (0 until partitions.length).map { i =>
      val s = ShuffleMapTask(stageId, stageAttemptId, partitions(i), localProperties,
        internalAccumulatorsSer, isFutureTask, rdds(i), deps(i), nextStageLocs)
      s.epoch = epoch
      s
    }.map(_.asInstanceOf[Task[Any]])
  }

  override def runTask(context: TaskContext): Array[MapStatus] = {
    throw new RuntimeException("BatchShuffleMapTasks should not be run!")
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "BatchShuffleMapTask(%d, %d)".format(stageId, partitionId)
}
