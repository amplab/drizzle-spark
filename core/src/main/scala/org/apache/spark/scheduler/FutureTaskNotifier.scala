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

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.storage.StorageLevel

/**
 * A FutureTask Notifier implements methods to push
 * MapStatus information about the shuffle blocks to the next stage
 */
private[spark] object FutureTaskNotifier extends Logging {

  def taskCompleted(
      status: MapStatus,
      mapId: Int,
      shuffleId: Int,
      numReduces: Int,
      nextStageLocs: Option[Seq[BlockManagerId]],
      shuffleWriteMetrics: ShuffleWriteMetrics,
      skipZeroByteNotifications: Boolean): Unit = {
    if (!nextStageLocs.isEmpty && numReduces == nextStageLocs.get.length) {
      val drizzleRpcsStart = System.nanoTime
      sendMapStatusToNextTaskLocations(status, mapId, shuffleId, numReduces, nextStageLocs,
        skipZeroByteNotifications)
      shuffleWriteMetrics.incWriteTime(System.nanoTime -
        drizzleRpcsStart)
    } else {
      logInfo(
        s"No taskCompletion next: ${nextStageLocs.map(_.length).getOrElse(0)} r: $numReduces")
    }
  }

  // Push metadata saying that this map task finished, so that the tasks in the next stage
  // know they can begin pulling the data.
  private def sendMapStatusToNextTaskLocations(
      status: MapStatus,
      mapId: Int,
      shuffleId: Int,
      numReduces: Int,
      nextStageLocs: Option[Seq[BlockManagerId]],
      skipZeroByteNotifications: Boolean) {
    val numReduces = nextStageLocs.get.length
    val uniqueLocations = if (skipZeroByteNotifications) {
      nextStageLocs.get.zipWithIndex.filter { x =>
        status.getSizeForBlock(x._2) != 0L
      }.map(_._1).toSet
    } else {
      nextStageLocs.get.toSet
    }
    uniqueLocations.foreach { blockManagerId =>
      try {
        SparkEnv.get.blockManager.blockTransferService.mapOutputReady(
          blockManagerId.host,
          blockManagerId.port,
          shuffleId,
          mapId,
          numReduces,
          status)
      } catch {
        case e: Exception =>
          logWarning(s"Failed to send map outputs to $blockManagerId", e)
      }
    }
  }

}
