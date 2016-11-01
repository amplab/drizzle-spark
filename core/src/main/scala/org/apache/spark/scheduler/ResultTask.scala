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
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.Properties

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rdd.RDD

/**
 * A task that sends back the output to the driver application.
 *
 * See [[Task]] for more information.
 *
 * @param stageId id of the stage this task belongs to
 * @param stageAttemptId attempt id of the stage this task belongs to
 * @param taskBinary broadcasted version of the serialized RDD and the function to apply on each
 *                   partition of the given RDD. Once deserialized, the type should be
 *                   (RDD[T], (TaskContext, Iterator[T]) => U).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 * @param outputId index of the task in this job (a job can launch tasks on only a subset of the
 *                 input RDD's partitions).
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 * @param serializedTaskMetrics a [[TaskMetrics]] that is created and serialized on the driver and
 *                              sent to executor side.
 *
 * The parameters below are optional:
 * @param jobId id of the job this task belongs to
 * @param appId id of the app this task belongs to
 * @param appAttemptId attempt id of the app this task belongs to
 */
private[spark] class ResultTask[T, U](
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    locs: Seq[TaskLocation],
    val outputId: Int,
    localProperties: Properties,
    serializedTaskMetrics: Array[Byte] =
      SparkEnv.get.closureSerializer.newInstance().serialize(TaskMetrics.registered).array(),
    isFutureTask: Boolean = false,
    depShuffleIds: Option[Seq[Seq[Int]]] = None,
    depShuffleNumMaps: Option[Seq[Int]] = None,
    jobId: Option[Int] = None,
    appId: Option[String] = None,
    appAttemptId: Option[String] = None)
  extends Task[U](stageId, stageAttemptId, partition.index,
    serializedTaskMetrics, localProperties, isFutureTask, depShuffleIds, depShuffleNumMaps,
    jobId, appId, appAttemptId)
  with Serializable {

  var rdd: RDD[T] = null
  var func: (TaskContext, Iterator[T]) => U = null

  @transient private[this] val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  override def prepTask(): Unit = {
    // Deserialize the RDD and the func using the broadcast variables.
    val threadMXBean = ManagementFactory.getThreadMXBean
    val deserializeStartTime = System.currentTimeMillis()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (_rdd, _func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    rdd = _rdd
    func = _func
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L
  }

  override def runTask(context: TaskContext): U = {
    // Deserialize the RDD and the func using the broadcast variables.
    if (func == null || rdd == null) {
      prepTask()
    }
    func(context, rdd.iterator(partition, context))
  }

  // This is only callable on the driver side.
  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ResultTask(" + stageId + ", " + partitionId + ")"
}

object ResultTask {

  def apply[T, U](
      stageId: Int,
      stageAttemptId: Int,
      partition: Partition,
      outputId: Int,
      localProperties: Properties,
      internalAccumulatorsSer: Array[Byte],
      isFutureTask: Boolean,
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U): ResultTask[T, U] = {
    val rt = new ResultTask[T, U](stageId, stageAttemptId, null, partition, Seq.empty, outputId,
      localProperties, internalAccumulatorsSer, isFutureTask)
    rt.rdd = rdd
    rt.func = func
    rt
  }

}
