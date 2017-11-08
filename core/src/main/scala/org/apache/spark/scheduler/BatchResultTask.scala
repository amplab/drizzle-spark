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
import org.apache.spark.rdd.RDD

private[spark] class BatchResultTask[T, U: ClassTag](
    stageId: Int,
    stageAttemptId: Int,
    taskBinaries: Broadcast[Array[Byte]],
    val partitions: Array[Partition],
    partitionId: Int,
    @transient private val locs: Seq[TaskLocation],
    val outputId: Int,
    localProperties: Properties,
    internalAccumulatorsSer: Array[Byte],
    isFutureTask: Boolean,
    depShuffleIds: Option[Seq[Seq[Int]]] = None,
    depShuffleNumMaps: Option[Seq[Int]] = None,
    jobId: Option[Int] = None,
    appId: Option[String] = None,
    appAttemptId: Option[String] = None)
  extends Task[Array[U]](stageId, stageAttemptId, partitionId,
      internalAccumulatorsSer, localProperties, isFutureTask, depShuffleIds, depShuffleNumMaps,
      jobId, appId, appAttemptId)
  with BatchTask
  with Serializable {

  @transient private[this] val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  var rdds: Array[RDD[T]] = null

  var funcs: Array[(TaskContext, Iterator[T]) => U] = null

  override def prepTask(): Unit = {
    // Deserialize the RDD and the func using the broadcast variables.
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rddI, funcI) =
      ser.deserialize[(Array[RDD[T]], Array[(TaskContext, Iterator[T]) => U])](
        ByteBuffer.wrap(taskBinaries.value), Thread.currentThread.getContextClassLoader)
    rdds = rddI
    funcs = funcI
  }

  // Called on the executor side to get a smaller tasks out
  def getTasks(): Seq[Task[Any]] = {
    if (rdds == null) {
      prepTask()
    }

    (0 until partitions.length).map { i =>
      val r = ResultTask(stageId, stageAttemptId, partitions(i), outputId, localProperties,
        internalAccumulatorsSer, isFutureTask, rdds(i), funcs(i))
      r.epoch = epoch
      r
    }.map(_.asInstanceOf[Task[Any]])
  }

  override def runTask(context: TaskContext): Array[U] = {
    throw new RuntimeException("BatchResultTasks should not be run!")
  }

  // This is only callable on the driver side.
  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "BatchResultTask(" + stageId + ", " + partitionId + ")"
}
