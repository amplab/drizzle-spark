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

import java.nio.ByteBuffer

import scala.collection.mutable
import scala.collection.mutable.HashSet
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.SerializableBuffer

/**
 * Description of a task that gets passed onto executors to be executed, usually created by
 * [[TaskSetManager.resourceOffer]].
 */
private[spark] class TaskDescription(
    val taskId: Long,
    val attemptNumber: Int,
    val executorId: String,
    val name: String,
    val index: Int,    // Index within this task's TaskSet
    val isFutureTask: Boolean,
    @transient private val _task: Task[_],
    @transient private val _addedFiles: mutable.Map[String, Long],
    @transient private val _addedJars: mutable.Map[String, Long],
    @transient private val _ser: SerializerInstance)
  extends Serializable with Logging {

  // Because ByteBuffers are not serializable, wrap the task in a SerializableBuffer
  private var buffer: SerializableBuffer = _

  def prepareSerializedTask(): Unit = {
    if (_task != null) {
      val serializedTask: ByteBuffer = try {
        Task.serializeWithDependencies(_task, _addedFiles, _addedJars, _ser)
      } catch {
        // If the task cannot be serialized, then there is not point in re-attempting
        // the task as it will always fail. So just abort the task set.
        case NonFatal(e) =>
          val msg = s"Failed to serialize the task $taskId, not attempting to retry it."
          logError(msg, e)
          // FIXME(shivaram): We dont have a handle to the taskSet here to abort it.
          throw new TaskNotSerializableException(e)
      }
      if (serializedTask.limit > TaskSetManager.TASK_SIZE_TO_WARN_KB * 1024) {
        logWarning(s"Stage ${_task.stageId} contains a task of very large size " +
          s"(${serializedTask.limit / 1024} KB). The maximum recommended task size is " +
          s"${TaskSetManager.TASK_SIZE_TO_WARN_KB} KB.")
      }
      buffer = new SerializableBuffer(serializedTask)
    } else {
      buffer = new SerializableBuffer(ByteBuffer.allocate(0))
    }
  }

  def serializedTask: ByteBuffer = buffer.value

  override def toString: String = "TaskDescription(TID=%d, index=%d)".format(taskId, index)
}
