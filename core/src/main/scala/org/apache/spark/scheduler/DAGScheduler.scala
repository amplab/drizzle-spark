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

import java.io.NotSerializableException
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec
import scala.collection.Map
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Stack}
import scala.concurrent.duration._
import scala.language.existentials
import scala.language.postfixOps
import scala.util.control.NonFatal

import org.apache.commons.lang3.SerializationUtils

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.partial.{ApproximateActionListener, ApproximateEvaluator, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.storage._
import org.apache.spark.storage.BlockManagerMessages.BlockManagerHeartbeat
import org.apache.spark.util._

/**
 * The high-level scheduling layer that implements stage-oriented scheduling. It computes a DAG of
 * stages for each job, keeps track of which RDDs and stage outputs are materialized, and finds a
 * minimal schedule to run the job. It then submits stages as TaskSets to an underlying
 * TaskScheduler implementation that runs them on the cluster. A TaskSet contains fully independent
 * tasks that can run right away based on the data that's already on the cluster (e.g. map output
 * files from previous stages), though it may fail if this data becomes unavailable.
 *
 * Spark stages are created by breaking the RDD graph at shuffle boundaries. RDD operations with
 * "narrow" dependencies, like map() and filter(), are pipelined together into one set of tasks
 * in each stage, but operations with shuffle dependencies require multiple stages (one to write a
 * set of map output files, and another to read those files after a barrier). In the end, every
 * stage will have only shuffle dependencies on other stages, and may compute multiple operations
 * inside it. The actual pipelining of these operations happens in the RDD.compute() functions of
 * various RDDs (MappedRDD, FilteredRDD, etc).
 *
 * In addition to coming up with a DAG of stages, the DAGScheduler also determines the preferred
 * locations to run each task on, based on the current cache status, and passes these to the
 * low-level TaskScheduler. Furthermore, it handles failures due to shuffle output files being
 * lost, in which case old stages may need to be resubmitted. Failures *within* a stage that are
 * not caused by shuffle file loss are handled by the TaskScheduler, which will retry each task
 * a small number of times before cancelling the whole stage.
 *
 * When looking through this code, there are several key concepts:
 *
 *  - Jobs (represented by [[ActiveJob]]) are the top-level work items submitted to the scheduler.
 *    For example, when the user calls an action, like count(), a job will be submitted through
 *    submitJob. Each Job may require the execution of multiple stages to build intermediate data.
 *
 *  - Stages ([[Stage]]) are sets of tasks that compute intermediate results in jobs, where each
 *    task computes the same function on partitions of the same RDD. Stages are separated at shuffle
 *    boundaries, which introduce a barrier (where we must wait for the previous stage to finish to
 *    fetch outputs). There are two types of stages: [[ResultStage]], for the final stage that
 *    executes an action, and [[ShuffleMapStage]], which writes map output files for a shuffle.
 *    Stages are often shared across multiple jobs, if these jobs reuse the same RDDs.
 *
 *  - Tasks are individual units of work, each sent to one machine.
 *
 *  - Cache tracking: the DAGScheduler figures out which RDDs are cached to avoid recomputing them
 *    and likewise remembers which shuffle map stages have already produced output files to avoid
 *    redoing the map side of a shuffle.
 *
 *  - Preferred locations: the DAGScheduler also computes where to run each task in a stage based
 *    on the preferred locations of its underlying RDDs, or the location of cached or shuffle data.
 *
 *  - Cleanup: all data structures are cleared when the running jobs that depend on them finish,
 *    to prevent memory leaks in a long-running application.
 *
 * To recover from failures, the same stage might need to run multiple times, which are called
 * "attempts". If the TaskScheduler reports that a task failed because a map output file from a
 * previous stage was lost, the DAGScheduler resubmits that lost stage. This is detected through a
 * CompletionEvent with FetchFailed, or an ExecutorLost event. The DAGScheduler will wait a small
 * amount of time to see whether other nodes or tasks fail, then resubmit TaskSets for any lost
 * stage(s) that compute the missing tasks. As part of this process, we might also have to create
 * Stage objects for old (finished) stages where we previously cleaned up the Stage object. Since
 * tasks from the old attempt of a stage could still be running, care must be taken to map any
 * events received in the correct Stage object.
 *
 * Here's a checklist to use when making or reviewing changes to this class:
 *
 *  - All data structures should be cleared when the jobs involving them end to avoid indefinite
 *    accumulation of state in long-running programs.
 *
 *  - When adding a new data structure, update `DAGSchedulerSuite.assertDataStructuresEmpty` to
 *    include the new structure. This will help to catch memory leaks.
 */
private[spark]
class DAGScheduler(
    private[scheduler] val sc: SparkContext,
    private[scheduler] val taskScheduler: TaskScheduler,
    listenerBus: LiveListenerBus,
    mapOutputTracker: MapOutputTrackerMaster,
    blockManagerMaster: BlockManagerMaster,
    env: SparkEnv,
    clock: Clock = new SystemClock())
  extends Logging {

  def this(sc: SparkContext, taskScheduler: TaskScheduler) = {
    this(
      sc,
      taskScheduler,
      sc.listenerBus,
      sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
      sc.env.blockManager.master,
      sc.env)
  }

  def this(sc: SparkContext) = this(sc, sc.taskScheduler)

  private[spark] val metricsSource: DAGSchedulerSource = new DAGSchedulerSource(this)

  private[scheduler] val nextJobId = new AtomicInteger(0)
  private[scheduler] def numTotalJobs: Int = nextJobId.get()
  private val nextStageId = new AtomicInteger(0)

  private[scheduler] val jobIdToStageIds = new HashMap[Int, HashSet[Int]]
  private[scheduler] val stageIdToStage = new HashMap[Int, Stage]
  /**
   * Mapping from shuffle dependency ID to the ShuffleMapStage that will generate the data for
   * that dependency. Only includes stages that are part of currently running job (when the job(s)
   * that require the shuffle stage complete, the mapping will be removed, and the only record of
   * the shuffle data will be in the MapOutputTracker).
   */
  private[scheduler] val shuffleIdToMapStage = new HashMap[Int, ShuffleMapStage]
  private[scheduler] val jobIdToActiveJob = new HashMap[Int, ActiveJob]

  // Stages we need to run whose parents aren't done. Only when not using future tasks.
  private[scheduler] val waitingStages = new HashSet[Stage]

  // Maps stage IDs to all of that stage's dependencies. This is used so that as soon as all of
  // the tasks for a given stage ID have been scheduled, we can schedule all of the dependencies
  // (we need the locations where the stage's tasks were scheduled in order to schedule the
  // dependencies). Only used when using drizzle.
  private[scheduler] val stageIdToDependencies = new HashMap[Int, HashSet[Stage]]

  // Maps stageIds to the ID of the executor where the task was launched. Ignores speculative
  // copies.
  val stageToTaskLocations = new HashMap[Int, Array[String]]

  // Stages we are running right now
  private[scheduler] val runningStages = new HashSet[Stage]

  // Stages that must be resubmitted due to fetch failures
  private[scheduler] val failedStages = new HashSet[Stage]

  private[scheduler] val activeJobs = new HashSet[ActiveJob]

  /**
   * Contains the locations that each RDD's partitions are cached on.  This map's keys are RDD ids
   * and its values are arrays indexed by partition numbers. Each array value is the set of
   * locations where that RDD partition is cached.
   *
   * All accesses to this map should be guarded by synchronizing on it (see SPARK-4454).
   */
  private val cacheLocs = new HashMap[Int, IndexedSeq[Seq[TaskLocation]]]

  // For tracking failed nodes, we use the MapOutputTracker's epoch number, which is sent with
  // every task. When we detect a node failing, we note the current epoch number and failed
  // executor, increment it for new tasks, and use this to ignore stray ShuffleMapTask results.
  //
  // TODO: Garbage collect information about failure epochs when we know there are no more
  //       stray messages to detect.
  private val failedEpoch = new HashMap[String, Long]

  private [scheduler] val outputCommitCoordinator = env.outputCommitCoordinator

  // A closure serializer that we reuse.
  // This is only safe because DAGScheduler runs in a single thread.
  private val closureSerializer = SparkEnv.get.closureSerializer.newInstance()

  /** If enabled, FetchFailed will not cause stage retry, in order to surface the problem. */
  private val disallowStageRetryForTest = sc.getConf.getBoolean("spark.test.noStageRetry", false)

  private val messageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("dag-scheduler-message")

  private[scheduler] val eventProcessLoop = new DAGSchedulerEventProcessLoop(this)
  taskScheduler.setDAGScheduler(this)

  /**
   * Called by the TaskSetManager to report task's starting.
   */
  def taskStarted(task: Task[_], taskInfo: TaskInfo) {
    eventProcessLoop.post(BeginEvent(task, taskInfo))
  }

  /**
   * Called by the TaskSetManager to report that a task has completed
   * and results are being fetched remotely.
   */
  def taskGettingResult(taskInfo: TaskInfo) {
    eventProcessLoop.post(GettingResultEvent(taskInfo))
  }

  /**
   * Called by the TaskSetManager to report task completions or failures.
   */
  def taskEnded(
      task: Task[_],
      reason: TaskEndReason,
      result: Any,
      accumUpdates: Seq[AccumulatorV2[_, _]],
      taskInfo: TaskInfo): Unit = {
    eventProcessLoop.post(
      CompletionEvent(task, reason, result, accumUpdates, taskInfo))
  }

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  def executorHeartbeatReceived(
      execId: String,
      // (taskId, stageId, stageAttemptId, accumUpdates)
      accumUpdates: Array[(Long, Int, Int, Seq[AccumulableInfo])],
      blockManagerId: BlockManagerId): Boolean = {
    listenerBus.post(SparkListenerExecutorMetricsUpdate(execId, accumUpdates))
    blockManagerMaster.driverEndpoint.askWithRetry[Boolean](
      BlockManagerHeartbeat(blockManagerId), new RpcTimeout(600 seconds, "BlockManagerHeartbeat"))
  }

  /**
   * Called by TaskScheduler implementation when an executor fails.
   */
  def executorLost(execId: String, reason: ExecutorLossReason): Unit = {
    eventProcessLoop.post(ExecutorLost(execId, reason))
  }

  /**
   * Called by TaskScheduler implementation when a host is added.
   */
  def executorAdded(execId: String, host: String): Unit = {
    eventProcessLoop.post(ExecutorAdded(execId, host))
  }

  /**
   * Called by the TaskSetManager to cancel an entire TaskSet due to either repeated failures or
   * cancellation of the job itself.
   */
  def taskSetFailed(taskSet: TaskSet, reason: String, exception: Option[Throwable]): Unit = {
    eventProcessLoop.post(TaskSetFailed(taskSet, reason, exception))
  }

  private[scheduler]
  def getCacheLocs(rdd: RDD[_]): IndexedSeq[Seq[TaskLocation]] = cacheLocs.synchronized {
    // Note: this doesn't use `getOrElse()` because this method is called O(num tasks) times
    if (!cacheLocs.contains(rdd.id)) {
      // Note: if the storage level is NONE, we don't need to get locations from block manager.
      val locs: IndexedSeq[Seq[TaskLocation]] = if (rdd.getStorageLevel == StorageLevel.NONE) {
        IndexedSeq.fill(rdd.partitions.length)(Nil)
      } else {
        val blockIds =
          rdd.partitions.indices.map(index => RDDBlockId(rdd.id, index)).toArray[BlockId]
        blockManagerMaster.getLocations(blockIds).map { bms =>
          bms.map(bm => TaskLocation(bm.host, bm.executorId))
        }
      }
      cacheLocs(rdd.id) = locs
    }
    cacheLocs(rdd.id)
  }

  private def clearCacheLocs(): Unit = cacheLocs.synchronized {
    cacheLocs.clear()
  }

  /**
   * Gets a shuffle map stage if one exists in shuffleIdToMapStage. Otherwise, if the
   * shuffle map stage doesn't already exist, this method will create the shuffle map stage in
   * addition to any missing ancestor shuffle map stages.
   */
  private def getOrCreateShuffleMapStage(
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int): ShuffleMapStage = {
    shuffleIdToMapStage.get(shuffleDep.shuffleId) match {
      case Some(stage) =>
        stage

      case None =>
        // Create stages for all missing ancestor shuffle dependencies.
        getMissingAncestorShuffleDependencies(shuffleDep.rdd).foreach { dep =>
          // Even though getMissingAncestorShuffleDependencies only returns shuffle dependencies
          // that were not already in shuffleIdToMapStage, it's possible that by the time we
          // get to a particular dependency in the foreach loop, it's been added to
          // shuffleIdToMapStage by the stage creation process for an earlier dependency. See
          // SPARK-13902 for more information.
          if (!shuffleIdToMapStage.contains(dep.shuffleId)) {
            createShuffleMapStage(dep, firstJobId)
          }
        }
        // Finally, create a stage for the given shuffle dependency.
        createShuffleMapStage(shuffleDep, firstJobId)
    }
  }

  /**
   * Creates a ShuffleMapStage that generates the given shuffle dependency's partitions. If a
   * previously run stage generated the same shuffle data, this function will copy the output
   * locations that are still available from the previous shuffle to avoid unnecessarily
   * regenerating data.
   */
  def createShuffleMapStage(shuffleDep: ShuffleDependency[_, _, _], jobId: Int): ShuffleMapStage = {
    val rdd = shuffleDep.rdd
    val numTasks = rdd.partitions.length
    val parents = getOrCreateParentStages(rdd, jobId)
    val id = nextStageId.getAndIncrement()
    val stage = new ShuffleMapStage(id, rdd, numTasks, parents, jobId, rdd.creationSite, shuffleDep)

    stageIdToStage(id) = stage
    shuffleIdToMapStage(shuffleDep.shuffleId) = stage
    updateJobIdStageIdMaps(jobId, stage)

    if (mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
      // A previously run stage generated partitions for this shuffle, so for each output
      // that's still available, copy information about that output location to the new stage
      // (so we don't unnecessarily re-compute that data).
      val serLocs = mapOutputTracker.getSerializedMapOutputStatuses(shuffleDep.shuffleId)
      val locs = MapOutputTracker.deserializeMapStatuses(serLocs)
      (0 until locs.length).foreach { i =>
        if (locs(i) ne null) {
          // locs(i) will be null if missing
          stage.addOutputLoc(i, locs(i))
        }
      }
    } else {
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      logInfo("Registering RDD " + rdd.id + " (" + rdd.getCreationSite + ") shuffle " +
        shuffleDep.shuffleId)
      mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.length)
    }
    stage
  }

  /**
   * Create a ResultStage associated with the provided jobId.
   */
  private def createResultStage(
      rdd: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      jobId: Int,
      callSite: CallSite): ResultStage = {
    val parents = getOrCreateParentStages(rdd, jobId)
    val id = nextStageId.getAndIncrement()
    val stage = new ResultStage(id, rdd, func, partitions, parents, jobId, callSite)
    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    stage
  }

  /**
   * Get or create the list of parent stages for a given RDD.  The new Stages will be created with
   * the provided firstJobId.
   */
  private def getOrCreateParentStages(rdd: RDD[_], firstJobId: Int): List[Stage] = {
    getShuffleDependencies(rdd).map { shuffleDep =>
      getOrCreateShuffleMapStage(shuffleDep, firstJobId)
    }.toList
  }

  /** Find ancestor shuffle dependencies that are not registered in shuffleToMapStage yet */
  private def getMissingAncestorShuffleDependencies(
      rdd: RDD[_]): Stack[ShuffleDependency[_, _, _]] = {
    val ancestors = new Stack[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.pop()
      if (!visited(toVisit)) {
        visited += toVisit
        getShuffleDependencies(toVisit).foreach { shuffleDep =>
          if (!shuffleIdToMapStage.contains(shuffleDep.shuffleId)) {
            ancestors.push(shuffleDep)
            waitingForVisit.push(shuffleDep.rdd)
          } // Otherwise, the dependency and its ancestors have already been registered.
        }
      }
    }
    ancestors
  }

  /**
   * Returns shuffle dependencies that are immediate parents of the given RDD.
   *
   * This function will not return more distant ancestors.  For example, if C has a shuffle
   * dependency on B which has a shuffle dependency on A:
   *
   * A <-- B <-- C
   *
   * calling this function with rdd C will only return the B <-- C dependency.
   *
   * This function is scheduler-visible for the purpose of unit testing.
   */
  private[scheduler] def getShuffleDependencies(
      rdd: RDD[_]): HashSet[ShuffleDependency[_, _, _]] = {
    val parents = new HashSet[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    val waitingForVisit = new Stack[RDD[_]]
    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.pop()
      if (!visited(toVisit)) {
        visited += toVisit
        toVisit.dependencies.foreach {
          case shuffleDep: ShuffleDependency[_, _, _] =>
            parents += shuffleDep
          case dependency =>
            waitingForVisit.push(dependency.rdd)
        }
      }
    }
    parents
  }

  private def getMissingParentStages(stage: Stage): List[Stage] = {
    val missing = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visited(rdd)) {
        visited += rdd
        val rddHasUncachedPartitions = getCacheLocs(rdd).contains(Nil)
        if (rddHasUncachedPartitions) {
          for (dep <- rdd.dependencies) {
            dep match {
              case shufDep: ShuffleDependency[_, _, _] =>
                val mapStage = getOrCreateShuffleMapStage(shufDep, stage.firstJobId)
                if (!mapStage.isAvailable) {
                  missing += mapStage
                }
              case narrowDep: NarrowDependency[_] =>
                waitingForVisit.push(narrowDep.rdd)
            }
          }
        }
      }
    }
    waitingForVisit.push(stage.rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    missing.toList
  }

  /**
   * Registers the given jobId among the jobs that need the given stage and
   * all of that stage's ancestors.
   */
  private def updateJobIdStageIdMaps(jobId: Int, stage: Stage): Unit = {
    @tailrec
    def updateJobIdStageIdMapsList(stages: List[Stage]) {
      if (stages.nonEmpty) {
        val s = stages.head
        s.jobIds += jobId
        jobIdToStageIds.getOrElseUpdate(jobId, new HashSet[Int]()) += s.id
        val parentsWithoutThisJobId = s.parents.filter { ! _.jobIds.contains(jobId) }
        updateJobIdStageIdMapsList(parentsWithoutThisJobId ++ stages.tail)
      }
    }
    updateJobIdStageIdMapsList(List(stage))
  }

  /**
   * Removes state for job and any stages that are not needed by any other job.  Does not
   * handle cancelling tasks or notifying the SparkListener about finished jobs/stages/tasks.
   *
   * @param job The job whose state to cleanup.
   */
  private def cleanupStateForJobAndIndependentStages(job: ActiveJob): Unit = {
    val registeredStages = jobIdToStageIds.get(job.jobId)
    if (registeredStages.isEmpty || registeredStages.get.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    } else {
      stageIdToStage.filterKeys(stageId => registeredStages.get.contains(stageId)).foreach {
        case (stageId, stage) =>
          val jobSet = stage.jobIds
          if (!jobSet.contains(job.jobId)) {
            logError(
              "Job %d not registered for stage %d even though that stage was registered for the job"
              .format(job.jobId, stageId))
          } else {
            def removeStage(stageId: Int) {
              // data structures based on Stage
              for (stage <- stageIdToStage.get(stageId)) {
                if (runningStages.contains(stage)) {
                  logDebug("Removing running stage %d".format(stageId))
                  runningStages -= stage
                }
                while ((shuffleIdToMapStage.find(_._2 == stage)).isDefined) {
                  for ((k, v) <- shuffleIdToMapStage.find(_._2 == stage)) {
                    shuffleIdToMapStage.remove(k)
                  }
                }
                if (stageIdToDependencies.contains(stage.id)) {
                  stageIdToDependencies -= stage.id
                }
                if (waitingStages.contains(stage)) {
                  logDebug("Removing stage %d from waiting set.".format(stageId))
                  waitingStages -= stage
                }
                if (failedStages.contains(stage)) {
                  logDebug("Removing stage %d from failed set.".format(stageId))
                  failedStages -= stage
                }
              }
              // data structures based on StageId
              stageIdToStage -= stageId
              logDebug("After removal of stage %d, remaining stages = %d"
                .format(stageId, stageIdToStage.size))
            }

            jobSet -= job.jobId
            if (jobSet.isEmpty) { // no other job needs this stage
              removeStage(stageId)
            }
          }
      }
    }
    jobIdToStageIds -= job.jobId
    jobIdToActiveJob -= job.jobId
    activeJobs -= job
    job.finalStage match {
      case r: ResultStage => r.removeActiveJob()
      case m: ShuffleMapStage => m.removeActiveJob(job)
    }
  }

  /**
   * Submit an action job to the scheduler.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   *   partitions of the target RDD, e.g. for operations like first()
   * @param callSite where in the user program this job was called
   * @param resultHandler callback to pass each result to
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   *
   * @return a JobWaiter object that can be used to block until the job finishes executing
   *         or can be used to cancel the job.
   *
   * @throws IllegalArgumentException when partitions ids are illegal
   */
  def submitJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): JobWaiter[U] = {
    // Check to make sure we are not launching a task on a partition that does not exist.
    val maxPartitions = rdd.partitions.length
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }

    val jobId = nextJobId.getAndIncrement()
    if (partitions.size == 0) {
      // Return immediately if the job is running 0 tasks
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitions.size > 0)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions.toArray, callSite, waiter,
      SerializationUtils.clone(properties)))
    waiter
  }

  /**
   * Run an action job on the given RDD and pass all the results to the resultHandler function as
   * they arrive.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   *   partitions of the target RDD, e.g. for operations like first()
   * @param callSite where in the user program this job was called
   * @param resultHandler callback to pass each result to
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   *
   * @throws Exception when the job fails
   */
  def runJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): Unit = {
    val start = System.nanoTime
    val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)
    // Note: Do not call Await.ready(future) because that calls `scala.concurrent.blocking`,
    // which causes concurrent SQL executions to fail if a fork-join pool is used. Note that
    // due to idiosyncrasies in Scala, `awaitPermission` is not actually used anywhere so it's
    // safe to pass in null here. For more detail, see SPARK-13747.
    val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
    waiter.completionFuture.ready(Duration.Inf)(awaitPermission)
    waiter.completionFuture.value.get match {
      case scala.util.Success(_) =>
        logInfo("Job %d finished: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
      case scala.util.Failure(exception) =>
        logInfo("Job %d failed: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
        // SPARK-8644: Include user stack trace in exceptions coming from DAGScheduler.
        val callerStackTrace = Thread.currentThread().getStackTrace.tail
        exception.setStackTrace(exception.getStackTrace ++ callerStackTrace)
        throw exception
    }
  }

  /**
   * Run an approximate job on the given RDD and pass all the results to an ApproximateEvaluator
   * as they arrive. Returns a partial result object from the evaluator.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param evaluator [[ApproximateEvaluator]] to receive the partial results
   * @param callSite where in the user program this job was called
   * @param timeout maximum time to wait for the job, in milliseconds
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   */
  def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      callSite: CallSite,
      timeout: Long,
      properties: Properties): PartialResult[R] = {
    val listener = new ApproximateActionListener(rdd, func, evaluator, timeout)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val partitions = (0 until rdd.partitions.length).toArray
    val jobId = nextJobId.getAndIncrement()
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions, callSite, listener, SerializationUtils.clone(properties)))
    listener.awaitResult()    // Will throw an exception if the job fails
  }


  /**
   * Batch scheduling API that allows users to schedule a batch of Jobs to be executed.
   */
  def submitManyJobs[T, U](jobs: Seq[JobDescription[T, U]]): BatchJobWaiter[U] = {
    // TODO: Check to make sure we are not launching a task on a partition that does not exist.
    val jobId = nextJobId.getAndIncrement()

    val partitions = jobs.head.partitions
    val resHandlers = jobs.map { j =>
      j.resultHandler
    }

    if (partitions.size == 0) {
      // Return immediately if the job is running 0 tasks
      return new BatchJobWaiter[U](this, jobId, 0, resHandlers)
    }

    val func2 = jobs.head.func.asInstanceOf[(TaskContext, Iterator[_]) => _]

    val funcs = jobs.tail.map { j =>
      val func2 = j.func.asInstanceOf[(TaskContext, Iterator[_]) => _]
      func2
    }

    val waiter = new BatchJobWaiter(this, jobId, partitions.size, resHandlers)
    eventProcessLoop.post(JobSubmitted(
      jobId, jobs.head.rdd, func2, partitions.toArray,
      jobs.head.callSite, waiter,
      SerializationUtils.clone(jobs.head.properties),
      jobs.tail.map(_.rdd), funcs))
    waiter
  }

  /**
   * Submit a shuffle map stage to run independently and get a JobWaiter object back. The waiter
   * can be used to block until the job finishes executing or can be used to cancel the job.
   * This method is used for adaptive query planning, to run map stages and look at statistics
   * about their outputs before submitting downstream stages.
   *
   * @param dependency the ShuffleDependency to run a map stage for
   * @param callback function called with the result of the job, which in this case will be a
   *   single MapOutputStatistics object showing how much data was produced for each partition
   * @param callSite where in the user program this job was submitted
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   */
  def submitMapStage[K, V, C](
      dependency: ShuffleDependency[K, V, C],
      callback: MapOutputStatistics => Unit,
      callSite: CallSite,
      properties: Properties): JobWaiter[MapOutputStatistics] = {

    val rdd = dependency.rdd
    val jobId = nextJobId.getAndIncrement()
    if (rdd.partitions.length == 0) {
      throw new SparkException("Can't run submitMapStage on RDD with 0 partitions")
    }

    // We create a JobWaiter with only one "task", which will be marked as complete when the whole
    // map stage has completed, and will be passed the MapOutputStatistics for that stage.
    // This makes it easier to avoid race conditions between the user code and the map output
    // tracker that might result if we told the user the stage had finished, but then they queries
    // the map output tracker and some node failures had caused the output statistics to be lost.
    val waiter = new JobWaiter(this, jobId, 1, (i: Int, r: MapOutputStatistics) => callback(r))
    eventProcessLoop.post(MapStageSubmitted(
      jobId, dependency, callSite, waiter, SerializationUtils.clone(properties)))
    waiter
  }

  /**
   * Cancel a job that is running or waiting in the queue.
   */
  def cancelJob(jobId: Int): Unit = {
    logInfo("Asked to cancel job " + jobId)
    eventProcessLoop.post(JobCancelled(jobId))
  }

  /**
   * Cancel all jobs in the given job group ID.
   */
  def cancelJobGroup(groupId: String): Unit = {
    logInfo("Asked to cancel job group " + groupId)
    eventProcessLoop.post(JobGroupCancelled(groupId))
  }

  /**
   * Cancel all jobs that are running or waiting in the queue.
   */
  def cancelAllJobs(): Unit = {
    eventProcessLoop.post(AllJobsCancelled)
  }

  private[scheduler] def doCancelAllJobs() {
    // Cancel all running jobs.
    runningStages.map(_.firstJobId).foreach(handleJobCancellation(_,
      reason = "as part of cancellation of all jobs"))
    activeJobs.clear() // These should already be empty by this point,
    jobIdToActiveJob.clear() // but just in case we lost track of some jobs...
  }

  /**
   * Cancel all jobs associated with a running or scheduled stage.
   */
  def cancelStage(stageId: Int) {
    eventProcessLoop.post(StageCancelled(stageId))
  }

  /**
   * Resubmit any failed stages. Ordinarily called after a small amount of time has passed since
   * the last fetch failure.
   */
  private[scheduler] def resubmitFailedStages() {
    if (failedStages.size > 0) {
      // Failed stages may be removed by job cancellation, so failed might be empty even if
      // the ResubmitFailedStages event has been scheduled.
      logInfo("Resubmitting failed stages")
      clearCacheLocs()
      val failedStagesCopy = failedStages.toArray
      failedStages.clear()
      for (stage <- failedStagesCopy.sortBy(_.firstJobId)) {
        submitStage(stage, None, scheduleBatchTasksAsNormal = true)
      }
    }
  }

  /**
   * Check for waiting stages which are now eligible for resubmission.
   * Submits stages that depend on the given parent stage. Called when the parent stage completes
   * successfully.
   */
  private def submitWaitingChildStages(parent: Stage) {
    logTrace(s"Checking if any dependencies of $parent are now runnable")
    logTrace("running: " + runningStages)
    logTrace("waiting: " + waitingStages)
    logTrace("failed: " + failedStages)
    val childStages = waitingStages.filter(_.parents.contains(parent)).toArray
    waitingStages --= childStages
    for (stage <- childStages.sortBy(_.firstJobId)) {
      submitStage(stage, None, scheduleBatchTasksAsNormal = true)
    }
  }

  /** Finds the earliest-created active job that needs the stage */
  // TODO: Probably should actually find among the active jobs that need this
  // stage the one with the highest priority (highest-priority pool, earliest created).
  // That should take care of at least part of the priority inversion problem with
  // cross-job dependencies.
  private def activeJobForStage(stage: Stage): Option[Int] = {
    val jobsThatUseStage: Array[Int] = stage.jobIds.toArray.sorted
    jobsThatUseStage.find(jobIdToActiveJob.contains)
  }

  private[scheduler] def handleJobGroupCancelled(groupId: String) {
    // Cancel all jobs belonging to this job group.
    // First finds all active jobs with this group id, and then kill stages for them.
    val activeInGroup = activeJobs.filter { activeJob =>
      Option(activeJob.properties).exists {
        _.getProperty(SparkContext.SPARK_JOB_GROUP_ID) == groupId
      }
    }
    val jobIds = activeInGroup.map(_.jobId)
    jobIds.foreach(handleJobCancellation(_, "part of cancelled job group %s".format(groupId)))
  }

  private[scheduler] def handleBeginEvent(task: Task[_], taskInfo: TaskInfo) {
    // Note that there is a chance that this task is launched after the stage is cancelled.
    // In that case, we wouldn't have the stage anymore in stageIdToStage.
    val stageAttemptId = stageIdToStage.get(task.stageId).map(_.latestInfo.attemptId).getOrElse(-1)
    listenerBus.post(SparkListenerTaskStart(task.stageId, stageAttemptId, taskInfo))

    // If stage exists and has batchRDDs in it
    if (stageIdToStage.get(task.stageId).map(!_.batchRDDs.isEmpty).getOrElse(false) &&
        task.isFutureTask) {
      // Keep track of where the task was scheduled so we can tell dependencies where to push
      // data to.
      val locationsForStage = stageToTaskLocations(task.stageId)

      // NOTE: locationsForStage only contains an array whose size is the number of tasks
      // launched. So for a stage retried this could be lesser than the number of partitions
      // Hence use taskInfo.index to index into this array
      locationsForStage(taskInfo.index) = taskInfo.executorId

      // Future tasks might be used here, so look at the the stages dependencies
      if (locationsForStage.filter(_ == null).length == 0) {
        // All of the stage's tasks have been scheduled, so schedule its dependencies.
        val stageId = task.stageId

        // Schedule all of the stage's dependencies.
        // TODO: If stageId isn't in here, we did something wrong. Fail gracefully.
        if (stageIdToDependencies.contains(stageId)) {
          val blockManagerIds = blockManagerMaster.getBlockManagerIdForExecutors(locationsForStage)
          logDebug(s"All tasks for stage $stageId have been scheduled, so scheduling " +
            s"dependencies using locations ${locationsForStage.mkString(",")} " +
            s"(translated to ${blockManagerIds.mkString(",")})")

          val dependencies = stageIdToDependencies(stageId)
          stageIdToDependencies.remove(stageId)
          dependencies.foreach(submitStage(_, Some(blockManagerIds)))
        }
      }
    }
  }

  private[scheduler] def handleTaskSetFailed(
      taskSet: TaskSet,
      reason: String,
      exception: Option[Throwable]): Unit = {
    stageIdToStage.get(taskSet.stageId).foreach { abortStage(_, reason, exception) }
  }

  private[scheduler] def cleanUpAfterSchedulerStop() {
    for (job <- activeJobs) {
      val error =
        new SparkException(s"Job ${job.jobId} cancelled because SparkContext was shut down")
      job.listener.jobFailed(error)
      // Tell the listeners that all of the running stages have ended.  Don't bother
      // cancelling the stages because if the DAG scheduler is stopped, the entire application
      // is in the process of getting stopped.
      val stageFailedMessage = "Stage cancelled because SparkContext was shut down"
      // The `toArray` here is necessary so that we don't iterate over `runningStages` while
      // mutating it.
      runningStages.toArray.foreach { stage =>
        markStageAsFinished(stage, Some(stageFailedMessage))
      }
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  private[scheduler] def handleGetTaskResult(taskInfo: TaskInfo) {
    listenerBus.post(SparkListenerTaskGettingResult(taskInfo))
  }

  private[scheduler] def handleJobSubmitted(jobId: Int,
      finalRDD: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      callSite: CallSite,
      listener: JobListener,
      properties: Properties,
      batchRDDs: Seq[RDD[_]],
      batchFuncs: Seq[(TaskContext, Iterator[_]) => _]) {
    val jobSubmissionTime = clock.getTimeMillis()
    var finalStage: ResultStage = null
    try {
      if (!batchRDDs.isEmpty) {
        val analysisBegin = System.nanoTime
        // Generate stage metadata for all stages. Then send that to our verify function
        val stageMetadata = analyzeStagesForRDD(finalRDD, func)
        val batchStageMetadata = batchRDDs.zip(batchFuncs).map(x => analyzeStagesForRDD(x._1, x._2))

        if (!verifyDrizzleBatching(stageMetadata, batchStageMetadata)) {
          logError(s"Drizzle batch verification failed. Got\n" +
            stageMetadata.mkString(",") + "\n" +
            batchStageMetadata.map(x => x.mkString(",")).mkString("\n"))
          throw new SparkException("Drizzle batch construction failed")
        }

        finalStage = createBatchStages(finalRDD, batchRDDs, stageMetadata, batchStageMetadata,
          func, batchFuncs, partitions, jobId, callSite)
        val analysisEnd = System.nanoTime
        logInfo("Analysis of Batch job took " + (analysisEnd-analysisBegin)/1e6 + " ms")
        // finalStage = newResultStage(finalRDD, func, partitions, jobId, callSite,
        //   batchRDDs, batchFuncs)
      } else {
        // New stage creation may throw an exception if, for example, jobs are run on a
        // HadoopRDD whose underlying HDFS files have been deleted.
        finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite)
      }
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }

    val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
    clearCacheLocs()
    logInfo("Got job %s (%s) with %d output partitions".format(
      job.jobId, callSite.shortForm, partitions.length))
    logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))

    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.setActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
    submitStage(finalStage)
  }

  private[scheduler] def handleMapStageSubmitted(jobId: Int,
      dependency: ShuffleDependency[_, _, _],
      callSite: CallSite,
      listener: JobListener,
      properties: Properties) {
    // Submitting this map stage might still require the creation of some parent stages, so make
    // sure that happens.
    var finalStage: ShuffleMapStage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      finalStage = getOrCreateShuffleMapStage(dependency, jobId)
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }

    val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
    clearCacheLocs()
    logInfo("Got map stage job %s (%s) with %d output partitions".format(
      jobId, callSite.shortForm, dependency.rdd.partitions.length))
    logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))

    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.addActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
    submitStage(finalStage, None)

    // If the whole stage has already finished, tell the listener and remove it
    if (finalStage.isAvailable) {
      markMapStageJobAsFinished(job, mapOutputTracker.getStatistics(dependency))
    }
  }

  private def submitStage(
      stage: Stage,
      locations: Option[Seq[BlockManagerId]] = None,
      scheduleBatchTasksAsNormal: Boolean = false) {
    // If the stage has any batchRDDs associated with it, then schedule
    // it using future tasks.
    if (!stage.batchRDDs.isEmpty && !scheduleBatchTasksAsNormal) {
      submitStageDrizzle(stage, locations)
    } else {
      submitStageNormal(stage)
    }
  }

  /** Submits stage, but first recursively submits any missing parents. */
  private def submitStageNormal(stage: Stage) {
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
      logDebug("submitStage(" + stage + ")")
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        val missing = getMissingParentStages(stage).sortBy(_.id)
        logDebug("missing: " + missing)
        if (missing.isEmpty) {
          logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
          submitMissingTasks(stage, jobId.get)
        } else {
          for (parent <- missing) {
            submitStage(parent, None, scheduleBatchTasksAsNormal = true)
          }
          waitingStages += stage
        }
      }
    } else {
      abortStage(stage, "No active job for stage " + stage.id, None)
    }
  }

  private def submitStageDrizzle(
      stage: Stage,
      locations: Option[Seq[BlockManagerId]]) {
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
      logDebug("submitStageDrizzle(" + stage + ")")
      if (!runningStages(stage) && !failedStages(stage)) {
        logDebug("Calling getMissingParentStages")
        val missing = getMissingParentStages(stage).sortBy(_.id)
        logDebug("Returned from getMissingParentStages")
        logDebug("missing: " + missing)
        if (!missing.isEmpty) {
          for (parent <- missing) {
            stageIdToDependencies.getOrElseUpdate(stage.id, new HashSet()).add(parent)
            // Don't need to recurse! The dependencies will be scheduled once all of the tasks in
            // stage have been scheduled.
          }
        }

        // Schedule the stage (maybe as future tasks).
        // Force future task scheduling when we have an unmet dependency.
        val scheduleAsFutureTasksDep = !missing.isEmpty
        logInfo(s"Submitting stage ${stage.id} futureTask ? $scheduleAsFutureTasksDep")
        submitMissingTasks(stage, jobId.get, scheduleAsFutureTasksDep, locations)
      } else {
        logInfo(s"Didn't submit stage ${stage.id} because it was already in " +
          s"running ($runningStages) or failed ($failedStages)")
      }
    } else {
      abortStage(stage, "No active job for stage " + stage.id, None)
    }
  }

  /** Called when stage's parents are available and we can now do its task. */
  // The stage's tasks may be scheduled as FutureTasks because the dependencies
  // are not available yet.
  private def submitMissingTasks(
      stage: Stage,
      jobId: Int,
      scheduleAsFutureTasksDep: Boolean = false,
      locations: Option[Seq[BlockManagerId]] = None) {
    logDebug("submitMissingTasks(" + stage + ")")
    val taskSubmissionBegin = System.nanoTime()
    // Get our pending tasks and remember them in our pendingTasks entry
    stage.pendingPartitions.clear()

    // First figure out the indexes of partition ids to compute.
    val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()

    // Use the scheduling pool, job group, description, etc. from an ActiveJob associated
    // with this Stage
    val properties = jobIdToActiveJob(jobId).properties

    runningStages += stage
    // SparkListenerStageSubmitted should be posted before testing whether tasks are
    // serializable. If tasks are not serializable, a SparkListenerStageCompleted event
    // will be posted, which should always come after a corresponding SparkListenerStageSubmitted
    // event.
    stage match {
      case s: ShuffleMapStage =>
        outputCommitCoordinator.stageStart(stage = s.id, maxPartitionId = s.numPartitions - 1)
      case s: ResultStage =>
        outputCommitCoordinator.stageStart(
          stage = s.id, maxPartitionId = s.rdd.partitions.length - 1)
    }

    val taskIdToLocations: Map[Int, Seq[TaskLocation]] = if (scheduleAsFutureTasksDep) {
      // Future tasks have no preferred locations.
      partitionsToCompute.map { id => (id, Nil) }.toMap
    } else {
      try {
        stage match {
          case s: ShuffleMapStage =>
            partitionsToCompute.map { id => (id, getPreferredLocs(stage.rdd, id))}.toMap
          case s: ResultStage =>
            val job = s.activeJob.get
            partitionsToCompute.map { id =>
              val p = s.partitions(id)
              (id, getPreferredLocs(stage.rdd, p))
            }.toMap
        }
      } catch {
        case NonFatal(e) =>
          stage.makeNewStageAttempt(partitionsToCompute.size)
          listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))
          abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
          runningStages -= stage
          return
      }
    }

    if (Utils.isDynamicAllocationEnabled(env.conf)) {
      stage.makeNewStageAttempt(partitionsToCompute.size, taskIdToLocations.values.toSeq)
    } else {
      stage.makeNewStageAttempt(partitionsToCompute.size)
    }

    listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))

    // TODO: Maybe we can keep the taskBinary in Stage to avoid serializing it multiple times.
    // Broadcasted binary for the task, used to dispatch tasks to executors. Note that we broadcast
    // the serialized copy of the RDD and for each task we will deserialize it, which means each
    // task gets a different copy of the RDD. This provides stronger isolation between tasks that
    // might modify state of objects referenced in their closures. This is necessary in Hadoop
    // where the JobConf/Configuration object is not thread-safe.
    var taskBinary: Broadcast[Array[Byte]] = null
    var taskBinaries: Broadcast[Array[Byte]] = null
    if (stage.batchRDDs.isEmpty) {
      try {
        // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
        // For ResultTask, serialize and broadcast (rdd, func).
        val taskBinaryBytes: Array[Byte] = stage match {
          case stage: ShuffleMapStage =>
            JavaUtils.bufferToArray(
              closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef))
          case stage: ResultStage =>
            JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.func): AnyRef))
        }

        taskBinary = sc.broadcast(taskBinaryBytes)
      } catch {
        // In the case of a failure during serialization, abort the stage.
        case e: NotSerializableException =>
          abortStage(stage, "Task not serializable: " + e.toString, Some(e))
          runningStages -= stage

          // Abort execution
          return
        case NonFatal(e) =>
          abortStage(stage, s"Task serialization failed: $e\n${Utils.exceptionString(e)}", Some(e))
          runningStages -= stage
          return
      }
    } else {
      // Serialize batch of data
      try {
        // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
        // For ResultTask, serialize and broadcast (rdd, func).
        val taskBinaryBytes: Array[Byte] = stage match {
          case stage: ShuffleMapStage =>
            closureSerializer.serialize(
              ((Seq(stage.rdd) ++ stage.batchRDDs).toArray,
               (Seq(stage.shuffleDep) ++ stage.batchDeps).toArray): AnyRef).array()
          case stage: ResultStage =>
            closureSerializer.serialize(
              ((Seq(stage.rdd) ++ stage.batchRDDs).toArray,
               (Seq(stage.func) ++ stage.batchFuncs).toArray): AnyRef).array()
        }
        taskBinaries = sc.broadcast(taskBinaryBytes)
      } catch {
        // In the case of a failure during serialization, abort the stage.
        case e: NotSerializableException =>
          abortStage(stage, "Task not serializable: " + e.toString, Some(e))
          runningStages -= stage

          // Abort execution
          return
        case NonFatal(e) =>
          abortStage(stage, s"Task serialization failed: $e\n${e.getStackTraceString}", Some(e))
          runningStages -= stage
          return
      }
    }

    val dependentShuffleIds: Option[Seq[Seq[Int]]] = if (scheduleAsFutureTasksDep) {
      Some(stage.parents.filter(x => x.isInstanceOf[ShuffleMapStage]).map { parentStage =>
        parentStage match {
          case sm: ShuffleMapStage => sm.getShuffleIds()
          case _ => Seq.empty
        }
      }.toSeq)
    } else {
      None
    }

    val dependentShuffleNumMaps: Option[Seq[Int]] = if (scheduleAsFutureTasksDep) {
      Some(stage.parents.filter(x => x.isInstanceOf[ShuffleMapStage]).map(st => st.numPartitions))
    } else {
      None
    }

    val tasks: Seq[Task[_]] = try {
      val serializedTaskMetrics = closureSerializer.serialize(stage.latestInfo.taskMetrics).array()
      stage match {
        case stage: ShuffleMapStage =>
          partitionsToCompute.map { id =>
            val locs = taskIdToLocations(id)
            val part = stage.rdd.partitions(id)
            if (stage.batchRDDs.isEmpty) {
              new ShuffleMapTask(stage.id, stage.latestInfo.attemptId,
                taskBinary, part, locs, properties, serializedTaskMetrics, scheduleAsFutureTasksDep,
                locations, dependentShuffleIds, dependentShuffleNumMaps,
                Option(jobId), Option(sc.applicationId), sc.applicationAttemptId)
            } else {
              new BatchShuffleMapTask(stage.id, stage.latestInfo.attemptId,
                taskBinaries, (Seq(part) ++ stage.batchRDDs.map(x => x.partitions(id))).toArray,
                part.index, locs, serializedTaskMetrics, properties, scheduleAsFutureTasksDep,
                locations, dependentShuffleIds, dependentShuffleNumMaps,
                Option(jobId), Option(sc.applicationId), sc.applicationAttemptId)
            }
          }

        case stage: ResultStage =>
          partitionsToCompute.map { id =>
            val p: Int = stage.partitions(id)
            val part = stage.rdd.partitions(p)
            val locs = taskIdToLocations(id)
            if (stage.batchRDDs.isEmpty) {
              new ResultTask(stage.id, stage.latestInfo.attemptId,
                taskBinary, part, locs, id, properties, serializedTaskMetrics,
                scheduleAsFutureTasksDep, dependentShuffleIds, dependentShuffleNumMaps,
                Option(jobId), Option(sc.applicationId), sc.applicationAttemptId)
            } else {
              new BatchResultTask(stage.id, stage.latestInfo.attemptId,
                taskBinaries, (Seq(part) ++ stage.batchRDDs.map(x => x.partitions(p))).toArray,
                part.index, locs, id, properties, serializedTaskMetrics,
                scheduleAsFutureTasksDep, dependentShuffleIds, dependentShuffleNumMaps,
                Option(jobId), Option(sc.applicationId), sc.applicationAttemptId)
            }
          }
      }
    } catch {
      case NonFatal(e) =>
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    // if (!stage.batchRDDs.isEmpty) {
    stageToTaskLocations(stage.id) = new Array[String](tasks.length)
    // }

    val taskSubmissionEnd = System.nanoTime()

    if (tasks.size > 0) {
      logInfo("Submitting " + tasks.size + " missing tasks from " + stage + " (" +
        stage.rdd + ")" + " took " + (taskSubmissionEnd-taskSubmissionBegin)/1e6 + "ms")
      stage.pendingPartitions ++= tasks.map(_.partitionId)
      logDebug("New pending partitions: " + stage.pendingPartitions)
      taskScheduler.submitTasks(new TaskSet(
        tasks.toArray, stage.id, stage.latestInfo.attemptId, jobId, properties,
        scheduleAsFutureTasksDep))
      stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
    } else {
      // Because we posted SparkListenerStageSubmitted earlier, we should mark
      // the stage as completed here in case there are no tasks to run
      markStageAsFinished(stage, None)

      val debugString = stage match {
        case stage: ShuffleMapStage =>
          s"Stage ${stage} is actually done; " +
            s"(available: ${stage.isAvailable}," +
            s"available outputs: ${stage.numAvailableOutputs}," +
            s"partitions: ${stage.numPartitions})"
        case stage : ResultStage =>
          s"Stage ${stage} is actually done; (partitions: ${stage.numPartitions})"
      }
      logDebug(debugString)

      submitWaitingChildStages(stage)
    }
  }

  /**
   * Merge local values from a task into the corresponding accumulators previously registered
   * here on the driver.
   *
   * Although accumulators themselves are not thread-safe, this method is called only from one
   * thread, the one that runs the scheduling loop. This means we only handle one task
   * completion event at a time so we don't need to worry about locking the accumulators.
   * This still doesn't stop the caller from updating the accumulator outside the scheduler,
   * but that's not our problem since there's nothing we can do about that.
   */
  private def updateAccumulators(event: CompletionEvent): Unit = {
    val task = event.task
    val stage = stageIdToStage(task.stageId)
    try {
      event.accumUpdates.foreach { updates =>
        val id = updates.id
        // Find the corresponding accumulator on the driver and update it
        val acc: AccumulatorV2[Any, Any] = AccumulatorContext.get(id) match {
          case Some(accum) => accum.asInstanceOf[AccumulatorV2[Any, Any]]
          case None =>
            throw new SparkException(s"attempted to access non-existent accumulator $id")
        }
        acc.merge(updates.asInstanceOf[AccumulatorV2[Any, Any]])
        // To avoid UI cruft, ignore cases where value wasn't updated
        if (acc.name.isDefined && !updates.isZero) {
          stage.latestInfo.accumulables(id) = acc.toInfo(None, Some(acc.value))
          event.taskInfo.accumulables += acc.toInfo(Some(updates.value), Some(acc.value))
        }
      }
    } catch {
      case NonFatal(e) =>
        logError(s"Failed to update accumulators for task ${task.partitionId}", e)
    }
  }

  private def handleFailedFutureTasks(task: Task[_], errorMessage: String) {
    // If this was a stage that ran as future tasks then put it in
    // failedStages so that it can be resubmitted after its parent stage completes.
    if (task.isFutureTask) {
      val failedStage = stageIdToStage(task.stageId)
      if (failedStage.latestInfo.attemptId != task.stageAttemptId) {
        logInfo(s"Ignoring failure from $task as it's from $failedStage attempt" +
          s" ${task.stageAttemptId} and there is a more recent attempt for that stage " +
          s"(attempt ID ${failedStage.latestInfo.attemptId}) running")
      } else {
        if (runningStages.contains(failedStage)) {
          logInfo(s"Marking $failedStage (${failedStage.name}) as failed " +
            s"due to a future task failure")
          markStageAsFinished(failedStage, Some(errorMessage))
        }
        logInfo(s"Adding $failedStage (${failedStage name}) to failedStages " +
          "due to future task failure")

        if (failedStages.isEmpty) {
          messageScheduler.schedule(new Runnable {
            override def run(): Unit = eventProcessLoop.post(ResubmitFailedStages)
          }, DAGScheduler.RESUBMIT_TIMEOUT, TimeUnit.MILLISECONDS)
        }
        failedStages += failedStage
      }
    }
  }

  /**
   * Responds to a task finishing. This is called inside the event loop so it assumes that it can
   * modify the scheduler's internal state. Use taskEnded() to post a task end event from outside.
   */
  private[scheduler] def handleTaskCompletion(event: CompletionEvent) {
    val task = event.task
    val taskId = event.taskInfo.id
    val stageId = task.stageId
    val taskType = Utils.getFormattedClassName(task)

    outputCommitCoordinator.taskCompleted(
      stageId,
      task.partitionId,
      event.taskInfo.attemptNumber, // this is a task attempt number
      event.reason)

    // Reconstruct task metrics. Note: this may be null if the task has failed.
    val taskMetrics: TaskMetrics =
      if (event.accumUpdates.nonEmpty) {
        try {
          TaskMetrics.fromAccumulators(event.accumUpdates)
        } catch {
          case NonFatal(e) =>
            logError(s"Error when attempting to reconstruct metrics for task $taskId", e)
            null
        }
      } else {
        null
      }

    // The stage may have already finished when we get this event -- eg. maybe it was a
    // speculative task. It is important that we send the TaskEnd event in any case, so listeners
    // are properly notified and can chose to handle it. For instance, some listeners are
    // doing their own accounting and if they don't get the task end event they think
    // tasks are still running when they really aren't.
    listenerBus.post(SparkListenerTaskEnd(
       stageId, task.stageAttemptId, taskType, event.reason, event.taskInfo, taskMetrics))

    if (!stageIdToStage.contains(task.stageId)) {
      // Skip all the actions if the stage has been cancelled.
      return
    }

    val stage = stageIdToStage(task.stageId)
    event.reason match {
      case Success =>
        stage.pendingPartitions -= task.partitionId
        task match {
          case rt: ResultTask[_, _] =>
            // Cast to ResultStage here because it's part of the ResultTask
            // TODO Refactor this out to a function that accepts a ResultStage
            val resultStage = stage.asInstanceOf[ResultStage]
            resultStage.activeJob match {
              case Some(job) =>
                if (!job.finished(rt.outputId)) {
                  updateAccumulators(event)
                  job.finished(rt.outputId) = true
                  job.numFinished += 1
                  // If the whole job has finished, remove it
                  if (job.numFinished == job.numPartitions) {
                    markStageAsFinished(resultStage)
                    cleanupStateForJobAndIndependentStages(job)
                    listenerBus.post(
                      SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
                  }

                  // taskSucceeded runs some user code that might throw an exception. Make sure
                  // we are resilient against that.
                  try {
                    job.listener.taskSucceeded(rt.outputId, event.result)
                  } catch {
                    case e: Exception =>
                      // TODO: Perhaps we want to mark the resultStage as failed?
                      job.listener.jobFailed(new SparkDriverExecutionException(e))
                  }
                }
              case None =>
                logInfo("Ignoring result from " + rt + " because its job has finished")
            }

          case brt: BatchResultTask[_, _] =>
            val resultStage = stage.asInstanceOf[ResultStage]
            resultStage.activeJob match {
              case Some(job) =>
                if (!job.finished(brt.outputId)) {
                  updateAccumulators(event)
                  job.finished(brt.outputId) = true
                  logInfo(s"Marking result from " + brt + " for output " + brt.outputId +
                    " as finished")
                  job.numFinished += 1
                  // If the whole job has finished, remove it
                  if (job.numFinished == job.numPartitions) {
                    markStageAsFinished(resultStage)
                    cleanupStateForJobAndIndependentStages(job)
                    listenerBus.post(
                      SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
                  }

                  // taskSucceeded runs some user code that might throw an exception. Make sure
                  // we are resilient against that.
                  try {
                    job.listener.taskSucceeded(brt.outputId, event.result)
                  } catch {
                    case e: Exception =>
                      e.printStackTrace()
                      // TODO: Perhaps we want to mark the resultStage as failed?
                      job.listener.jobFailed(new SparkDriverExecutionException(e))
                  }
                } else {
                  logInfo(s"Ignoring result from " + brt + " for output " + brt.outputId +
                    " as it is marked as finished")
                }
              case None =>
                logInfo("Ignoring result from " + brt + " because its job has finished")
            }

          case smt: ShuffleMapTask =>
            val shuffleStage = stage.asInstanceOf[ShuffleMapStage]
            updateAccumulators(event)
            val status = event.result.asInstanceOf[MapStatus]
            val execId = status.location.executorId
            logDebug("ShuffleMapTask finished on " + execId)
            if (failedEpoch.contains(execId) && smt.epoch <= failedEpoch(execId)) {
              logInfo(s"Ignoring possibly bogus $smt completion from executor $execId")
            } else {
              shuffleStage.addOutputLoc(smt.partitionId, status)
            }

            if (runningStages.contains(shuffleStage) && shuffleStage.pendingPartitions.isEmpty) {
              markStageAsFinished(shuffleStage)
              logInfo("looking for newly runnable stages")
              logInfo("running: " + runningStages)
              logInfo("waiting: " + waitingStages)
              logInfo("queued: " + stageIdToDependencies)
              logInfo("failed: " + failedStages)

              // We supply true to increment the epoch number here in case this is a
              // recomputation of the map outputs. In that case, some nodes may have cached
              // locations with holes (from when we detected the error) and will need the
              // epoch incremented to refetch them.
              // TODO: Only increment the epoch number if this is not the first time
              //       we registered these map outputs.
              mapOutputTracker.registerMapOutputs(
                shuffleStage.shuffleDep.shuffleId,
                shuffleStage.outputLocInMapOutputTrackerFormat(),
                changeEpoch = true)

              clearCacheLocs()

              if (!shuffleStage.isAvailable) {
                // Some tasks had failed; let's resubmit this shuffleStage
                // TODO: Lower-level scheduler should also deal with this
                logInfo("Resubmitting " + shuffleStage + " (" + shuffleStage.name +
                  ") because some of its tasks had failed: " +
                  shuffleStage.findMissingPartitions().mkString(", "))
                submitStage(shuffleStage)
              } else {
                // Mark any map-stage jobs waiting on this stage as finished
                if (shuffleStage.mapStageJobs.nonEmpty) {
                  val stats = mapOutputTracker.getStatistics(shuffleStage.shuffleDep)
                  for (job <- shuffleStage.mapStageJobs) {
                    markMapStageJobAsFinished(job, stats)
                  }
                }
                submitWaitingChildStages(shuffleStage)
              }
            }

          case bsmt: BatchShuffleMapTask =>
            val shuffleStage = stage.asInstanceOf[ShuffleMapStage]
            updateAccumulators(event)
            val status = event.result.asInstanceOf[Array[Object]].map(_.asInstanceOf[MapStatus])
            val execId = status.head.location.executorId
            if (failedEpoch.contains(execId) && bsmt.epoch <= failedEpoch(execId)) {
              logInfo(s"Ignoring possibly bogus $bsmt completion from executor $execId")
            } else {
              shuffleStage.addOutputLoc(bsmt.partitionId, status.head, status.tail.toSeq)
            }

            if (runningStages.contains(shuffleStage) && shuffleStage.pendingPartitions.isEmpty) {
              markStageAsFinished(shuffleStage)
              logInfo("looking for newly runnable stages")
              logInfo("running: " + runningStages)
              logInfo("waiting: " + waitingStages)
              logInfo("queued: " + stageIdToDependencies)
              logInfo("failed: " + failedStages)

              // We supply true to increment the epoch number here in case this is a
              // recomputation of the map outputs. In that case, some nodes may have cached
              // locations with holes (from when we detected the error) and will need the
              // epoch incremented to refetch them.
              // TODO: Only increment the epoch number if this is not the first time
              //       we registered these map outputs.
              mapOutputTracker.registerMapOutputs(
                shuffleStage.shuffleDep.shuffleId,
                shuffleStage.outputLocInMapOutputTrackerFormat(),
                changeEpoch = true)

              val batchOuts = shuffleStage.batchOutputsInMapOutputTrackerFormat()
              shuffleStage.batchDeps.zip(batchOuts).foreach { case (dep, out) =>
                mapOutputTracker.registerMapOutputs(
                  dep.shuffleId,
                  out,
                  changeEpoch = false)
              }

              clearCacheLocs()

              if (!shuffleStage.isAvailable) {
                // Some tasks had failed; let's resubmit this shuffleStage
                // TODO: Lower-level scheduler should also deal with this
                logInfo("Resubmitting " + shuffleStage + " (" + shuffleStage.name +
                  ") because some of its tasks had failed: " +
                  shuffleStage.findMissingPartitions().mkString(", "))
                submitStage(shuffleStage)
              } else {
                // Mark any map-stage jobs waiting on this stage as finished
                if (shuffleStage.mapStageJobs.nonEmpty) {
                  val stats = mapOutputTracker.getStatistics(shuffleStage.shuffleDep)
                  for (job <- shuffleStage.mapStageJobs) {
                    markMapStageJobAsFinished(job, stats)
                  }
                }
              }

              // Note: newly runnable stages will be submitted below when we submit waiting stages
            }

        }

      case Resubmitted =>
        logInfo("Resubmitted " + task + ", so marking it as still running")
        stage.pendingPartitions += task.partitionId

      case FetchFailed(bmAddress, shuffleId, mapId, reduceId, failureMessage) =>
        val failedStage = stageIdToStage(task.stageId)
        val mapStage = shuffleIdToMapStage(shuffleId)

        if (failedStage.latestInfo.attemptId != task.stageAttemptId) {
          logInfo(s"Ignoring fetch failure from $task as it's from $failedStage attempt" +
            s" ${task.stageAttemptId} and there is a more recent attempt for that stage " +
            s"(attempt ID ${failedStage.latestInfo.attemptId}) running")
        } else {
          // It is likely that we receive multiple FetchFailed for a single stage (because we have
          // multiple tasks running concurrently on different executors). In that case, it is
          // possible the fetch failure has already been handled by the scheduler.
          if (runningStages.contains(failedStage)) {
            logInfo(s"Marking $failedStage (${failedStage.name}) as failed " +
              s"due to a fetch failure from $mapStage (${mapStage.name})")
            markStageAsFinished(failedStage, Some(failureMessage))
          } else {
            logDebug(s"Received fetch failure from $task, but its from $failedStage which is no " +
              s"longer running")
          }

          if (disallowStageRetryForTest) {
            abortStage(failedStage, "Fetch failure will not retry stage due to testing config",
              None)
          } else if (failedStage.failedOnFetchAndShouldAbort(task.stageAttemptId)) {
            abortStage(failedStage, s"$failedStage (${failedStage.name}) " +
              s"has failed the maximum allowable number of " +
              s"times: ${Stage.MAX_CONSECUTIVE_FETCH_FAILURES}. " +
              s"Most recent failure reason: ${failureMessage}", None)
          } else {
            if (failedStages.isEmpty) {
              // Don't schedule an event to resubmit failed stages if failed isn't empty, because
              // in that case the event will already have been scheduled.
              // TODO: Cancel running tasks in the stage
              logInfo(s"Resubmitting $mapStage (${mapStage.name}) and " +
                s"$failedStage (${failedStage.name}) due to fetch failure")
              messageScheduler.schedule(new Runnable {
                override def run(): Unit = eventProcessLoop.post(ResubmitFailedStages)
              }, DAGScheduler.RESUBMIT_TIMEOUT, TimeUnit.MILLISECONDS)
            }
            failedStages += failedStage
            if (!mapStage.isAvailable) {
              failedStages += mapStage
            }
          }
          // Mark the map whose fetch failed as broken in the map stage
          if (mapId != -1) {
            mapStage.removeOutputLoc(mapId, bmAddress)
            mapOutputTracker.unregisterMapOutput(shuffleId, mapId, bmAddress)
          }

          // TODO: mark the executor as failed only if there were lots of fetch failures on it
          if (bmAddress != null) {
            handleExecutorLost(bmAddress.executorId, filesLost = true, Some(task.epoch))
          }
        }

      case commitDenied: TaskCommitDenied =>
        // Do nothing here, left up to the TaskScheduler to decide how to handle denied commits

      case exceptionFailure: ExceptionFailure =>
        handleFailedFutureTasks(task, exceptionFailure.toErrorString)
        // Tasks failed with exceptions might still have accumulator updates.
        updateAccumulators(event)

      case TaskResultLost =>
        // Do nothing here; the TaskScheduler handles these failures and resubmits the task.

      case e: ExecutorLostFailure =>
        handleFailedFutureTasks(task, e.toErrorString)

      case TaskKilled | UnknownReason =>
        // Unrecognized failure - also do nothing. If the task fails repeatedly, the TaskScheduler
        // will abort the job.
    }
  }

  /**
   * Responds to an executor being lost. This is called inside the event loop, so it assumes it can
   * modify the scheduler's internal state. Use executorLost() to post a loss event from outside.
   *
   * We will also assume that we've lost all shuffle blocks associated with the executor if the
   * executor serves its own blocks (i.e., we're not using external shuffle), the entire slave
   * is lost (likely including the shuffle service), or a FetchFailed occurred, in which case we
   * presume all shuffle data related to this executor to be lost.
   *
   * Optionally the epoch during which the failure was caught can be passed to avoid allowing
   * stray fetch failures from possibly retriggering the detection of a node as lost.
   */
  private[scheduler] def handleExecutorLost(
      execId: String,
      filesLost: Boolean,
      maybeEpoch: Option[Long] = None) {
    val currentEpoch = maybeEpoch.getOrElse(mapOutputTracker.getEpoch)
    if (!failedEpoch.contains(execId) || failedEpoch(execId) < currentEpoch) {
      failedEpoch(execId) = currentEpoch
      logInfo("Executor lost: %s (epoch %d)".format(execId, currentEpoch))
      blockManagerMaster.removeExecutor(execId)

      if (filesLost || !env.blockManager.externalShuffleServiceEnabled) {
        logInfo("Shuffle files lost for executor: %s (epoch %d)".format(execId, currentEpoch))
        // TODO: This will be really slow if we keep accumulating shuffle map stages
        for ((shuffleId, stage) <- shuffleIdToMapStage) {
          stage.removeOutputsOnExecutor(execId)
          mapOutputTracker.registerMapOutputs(
            shuffleId,
            stage.outputLocInMapOutputTrackerFormat(),
            changeEpoch = true)
        }
        if (shuffleIdToMapStage.isEmpty) {
          mapOutputTracker.incrementEpoch()
        }
        clearCacheLocs()
      }
    } else {
      logDebug("Additional executor lost message for " + execId +
               "(epoch " + currentEpoch + ")")
    }
  }

  private[scheduler] def handleExecutorAdded(execId: String, host: String) {
    // remove from failedEpoch(execId) ?
    if (failedEpoch.contains(execId)) {
      logInfo("Host added was in lost list earlier: " + host)
      failedEpoch -= execId
    }
  }

  private[scheduler] def handleStageCancellation(stageId: Int) {
    stageIdToStage.get(stageId) match {
      case Some(stage) =>
        val jobsThatUseStage: Array[Int] = stage.jobIds.toArray
        jobsThatUseStage.foreach { jobId =>
          handleJobCancellation(jobId, s"because Stage $stageId was cancelled")
        }
      case None =>
        logInfo("No active jobs to kill for Stage " + stageId)
    }
  }

  private[scheduler] def handleJobCancellation(jobId: Int, reason: String = "") {
    if (!jobIdToStageIds.contains(jobId)) {
      logDebug("Trying to cancel unregistered job " + jobId)
    } else {
      failJobAndIndependentStages(
        jobIdToActiveJob(jobId), "Job %d cancelled %s".format(jobId, reason))
    }
  }

  /**
   * Marks a stage as finished and removes it from the list of running stages.
   */
  private def markStageAsFinished(stage: Stage, errorMessage: Option[String] = None): Unit = {
    val serviceTime = stage.latestInfo.submissionTime match {
      case Some(t) => "%.03f".format((clock.getTimeMillis() - t) / 1000.0)
      case _ => "Unknown"
    }
    if (errorMessage.isEmpty) {
      logInfo("%s (%s) finished in %s s".format(stage, stage.name, serviceTime))
      stage.latestInfo.completionTime = Some(clock.getTimeMillis())

      // Clear failure count for this stage, now that it's succeeded.
      // We only limit consecutive failures of stage attempts,so that if a stage is
      // re-used many times in a long-running job, unrelated failures don't eventually cause the
      // stage to be aborted.
      stage.clearFailures()
    } else {
      stage.latestInfo.stageFailed(errorMessage.get)
      logInfo(s"$stage (${stage.name}) failed in $serviceTime s due to ${errorMessage.get}")
    }

    outputCommitCoordinator.stageEnd(stage.id)
    listenerBus.post(SparkListenerStageCompleted(stage.latestInfo))
    runningStages -= stage
  }

  /**
   * Aborts all jobs depending on a particular Stage. This is called in response to a task set
   * being canceled by the TaskScheduler. Use taskSetFailed() to inject this event from outside.
   */
  private[scheduler] def abortStage(
      failedStage: Stage,
      reason: String,
      exception: Option[Throwable]): Unit = {
    if (!stageIdToStage.contains(failedStage.id)) {
      // Skip all the actions if the stage has been removed.
      return
    }
    val dependentJobs: Seq[ActiveJob] =
      activeJobs.filter(job => stageDependsOn(job.finalStage, failedStage)).toSeq
    failedStage.latestInfo.completionTime = Some(clock.getTimeMillis())
    for (job <- dependentJobs) {
      failJobAndIndependentStages(job, s"Job aborted due to stage failure: $reason", exception)
    }
    if (dependentJobs.isEmpty) {
      logInfo("Ignoring failure of " + failedStage + " because all jobs depending on it are done")
    }
  }

  /** Fails a job and all stages that are only used by that job, and cleans up relevant state. */
  private def failJobAndIndependentStages(
      job: ActiveJob,
      failureReason: String,
      exception: Option[Throwable] = None): Unit = {
    val error = new SparkException(failureReason, exception.getOrElse(null))
    var ableToCancelStages = true

    val shouldInterruptThread =
      if (job.properties == null) false
      else job.properties.getProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false").toBoolean

    // Cancel all independent, running stages.
    val stages = jobIdToStageIds(job.jobId)
    if (stages.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    }
    stages.foreach { stageId =>
      val jobsForStage: Option[HashSet[Int]] = stageIdToStage.get(stageId).map(_.jobIds)
      if (jobsForStage.isEmpty || !jobsForStage.get.contains(job.jobId)) {
        logError(
          "Job %d not registered for stage %d even though that stage was registered for the job"
            .format(job.jobId, stageId))
      } else if (jobsForStage.get.size == 1) {
        if (!stageIdToStage.contains(stageId)) {
          logError(s"Missing Stage for stage with id $stageId")
        } else {
          // This is the only job that uses this stage, so fail the stage if it is running.
          val stage = stageIdToStage(stageId)
          if (runningStages.contains(stage)) {
            try { // cancelTasks will fail if a SchedulerBackend does not implement killTask
              taskScheduler.cancelTasks(stageId, shouldInterruptThread)
              markStageAsFinished(stage, Some(failureReason))
            } catch {
              case e: UnsupportedOperationException =>
                logInfo(s"Could not cancel tasks for stage $stageId", e)
              ableToCancelStages = false
            }
          }
        }
      }
    }

    if (ableToCancelStages) {
      // SPARK-15783 important to cleanup state first, just for tests where we have some asserts
      // against the state.  Otherwise we have a *little* bit of flakiness in the tests.
      cleanupStateForJobAndIndependentStages(job)
      job.listener.jobFailed(error)
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  /** Return true if one of stage's ancestors is target. */
  private def stageDependsOn(stage: Stage, target: Stage): Boolean = {
    if (stage == target) {
      return true
    }
    val visitedStages = new HashSet[Stage]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[Stage]
    def visit(st: Stage) {
      if (!visitedStages(st)) {
        visitedStages += st
        for (dep <- st.parents) {
          dep match {
            case mapStage: ShuffleMapStage =>
              if (!mapStage.isAvailable) {
                waitingForVisit.push(mapStage)
              }  // Otherwise there's no need to follow the dependency back
            case resStage: ResultStage =>
              waitingForVisit.push(resStage) // Can this ever happen ?
          }
        }
      }
    }
    waitingForVisit.push(stage)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    visitedStages.contains(target)
  }

  /**
   * Gets the locality information associated with a partition of a particular RDD.
   *
   * This method is thread-safe and is called from both DAGScheduler and SparkContext.
   *
   * @param rdd whose partitions are to be looked at
   * @param partition to lookup locality information for
   * @return list of machines that are preferred by the partition
   */
  private[spark]
  def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = {
    getPreferredLocsInternal(rdd, partition, new HashSet)
  }

  /**
   * Recursive implementation for getPreferredLocs.
   *
   * This method is thread-safe because it only accesses DAGScheduler state through thread-safe
   * methods (getCacheLocs()); please be careful when modifying this method, because any new
   * DAGScheduler state accessed by it may require additional synchronization.
   */
  private def getPreferredLocsInternal(
      rdd: RDD[_],
      partition: Int,
      visited: HashSet[(RDD[_], Int)]): Seq[TaskLocation] = {
    // If the partition has already been visited, no need to re-visit.
    // This avoids exponential path exploration.  SPARK-695
    if (!visited.add((rdd, partition))) {
      // Nil has already been returned for previously visited partitions.
      return Nil
    }
    // If the partition is cached, return the cache locations
    val cached = getCacheLocs(rdd)(partition)
    if (cached.nonEmpty) {
      return cached
    }
    // If the RDD has some placement preferences (as is the case for input RDDs), get those
    val rddPrefs = rdd.preferredLocations(rdd.partitions(partition)).toList
    if (rddPrefs.nonEmpty) {
      return rddPrefs.map(TaskLocation(_))
    }

    // If the RDD has narrow dependencies, pick the first partition of the first narrow dependency
    // that has any placement preferences. Ideally we would choose based on transfer sizes,
    // but this will do for now.
    rdd.dependencies.foreach {
      case n: NarrowDependency[_] =>
        for (inPart <- n.getParents(partition)) {
          val locs = getPreferredLocsInternal(n.rdd, inPart, visited)
          if (locs != Nil) {
            return locs
          }
        }

      case _ =>
    }

    Nil
  }

  private object StageType extends Enumeration {
    val SHUFFLE_MAP = Value("ShuffleMap")
    val RESULT = Value("Result")
  }

  // Metadata that describes a stage that would be created by Spark.
  // id - NOTE: This will not be the same as StageId eventually created
  // kind - ShuffleMap or Result
  // numTasks - number of tasks that will run
  private case class StageMetadata(
      id: Int,
      kind: StageType.Value,
      numTasks: Int,
      rdd: RDD[_],
      shuffleDep: Option[ShuffleDependency[_, _, _]],
      func: Option[(TaskContext, Iterator[_]) => _],
      deps: ArrayBuffer[Int]) {
    override def toString: String = s"""Stage(${id},${kind},RDD${rdd.id},${deps.mkString(",")})"""
  }

  // Walks up the dependency chain from this RDD and returns information
  // on stages that would get created if we submitted this RDD to be evaluated.
  // The information we return per-stage is
  //    a. The type of the stage -- ShuffleMap or ResultStage
  //    b. The number of tasks or partitions in the stage
  //    c. The dependencies for each stage -- i.e. what other stages they depend on
  //       or rather the "stage-edges" that exist.
  // The returned data structure is thus like a edge list in a stage graph
  // Notably this function is "constant" in that it doesn't mutate any internal
  // data structures in the DAGScheduler and can be run repeatedly.
  //
  // @return map from stage metadata id to StageMetadata
  private def analyzeStagesForRDD(
      rdd: RDD[_],
      func: (TaskContext, Iterator[_]) => _): HashMap[Int, StageMetadata] = {
    val stageMap = new HashMap[Int, StageMetadata]
    // Add a result stage for this RDD first
    stageMap.put(rdd.id,
      StageMetadata(rdd.id, StageType.RESULT, rdd.partitions.length,
        rdd, None, Some(func), new ArrayBuffer[Int]()))

    val visited = new HashSet[RDD[_]]
    val visitedShuffleDep = new HashMap[Int, Int]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[(RDD[_], Int)]

    def visit( rsid: (RDD[_], Int)) {
      val r = rsid._1
      val sid = rsid._2
      if (!visited(r)) {
        visited += r
        for (i <- 0 until r.dependencies.size) {
          val dep = r.dependencies(i)
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              if (!visitedShuffleDep.contains(shufDep.shuffleId)) {
                stageMap.put(shufDep.rdd.id, StageMetadata(shufDep.rdd.id, StageType.SHUFFLE_MAP,
                  shufDep.rdd.partitions.length, dep.rdd, Some(shufDep), None,
                  new ArrayBuffer[Int]()))
                // Add a link from current stage to this one
                stageMap(sid).deps += shufDep.rdd.id
                visitedShuffleDep.put(shufDep.shuffleId, shufDep.rdd.id)
                waitingForVisit.push( (dep.rdd, shufDep.rdd.id) )
              } else {
                val shuffleStageId = visitedShuffleDep(shufDep.shuffleId)
                waitingForVisit.push( (dep.rdd, shuffleStageId) )
              }
            case _ =>
              waitingForVisit.push( (dep.rdd, sid) )
          }
        }
      }
    }
    waitingForVisit.push( (rdd, rdd.id) )
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    stageMap
  }

  private def verifyDrizzleBatching(
      firstMetadata: HashMap[Int, StageMetadata],
      batchMetadata: Seq[HashMap[Int, StageMetadata]]): Boolean = {

    // Preliminaries:
    // Starting from the first stage remove the stage from future iterations as
    // if they will be computed by an earlier iteration.
    logDebug(s"Drizzle verify batch stages. Got " +
      firstMetadata.mkString(",") + "\n" +
      batchMetadata.map(x => x.mkString(",")).mkString("\n"))

    val rddsToRemove = new HashSet[Int]
    rddsToRemove ++= firstMetadata.keys
    batchMetadata.foreach { batch =>
      rddsToRemove.foreach { x =>
        batch.remove(x)
      }
      rddsToRemove ++= batch.keys
    }

    logDebug(s"Drizzle AFTER verify batch stages. Got\n" +
      firstMetadata.mkString(",") + "\n" +
      batchMetadata.map(x => x.mkString(",")).mkString("\n"))

    // Constraints we use to determine if the batch can be executed

    // 1. The number of stages generated for each iteration in the batch should be the same.
    val sameSize = batchMetadata.filter(x => x.size != firstMetadata.size)
    if (!sameSize.isEmpty) {
      logInfo(s"Iterations within a batch should have the same number of dependencies.")
      return false
    }

    // TODO: Replace this with a check for the dag of stages matching
    // i.e. start with result stage and walk up checking the kind of dependency at each step.
    // 2. Each stage in order should be of the same kind in each iteration.
    val sortedFirstStages = firstMetadata.values.toSeq.sortBy(x => x.id)
    val sortedBatchStages = batchMetadata.map { iter =>
      iter.values.toSeq.sortBy(x => x.id)
    }
    val sameKindStages = sortedBatchStages.map { iter =>
      iter.zip(sortedFirstStages).forall(x => x._1.kind == x._2.kind)
    }
    if (!sameKindStages.forall(x => x)) {
      logInfo(s"Iterations within a batch should have the same kind of dependencies.")
      return false
    }

    true
  }

  private def createDrizzleShuffleMapStage(
      rdd: RDD[_],
      batchRDDs: Seq[RDD[_]],
      shuffleDep: ShuffleDependency[_, _, _],
      batchDeps: Seq[ShuffleDependency[_, _, _]],
      firstJobId: Int,
      callSite: CallSite,
      parentStages: List[Stage]): ShuffleMapStage = {
    val id = nextStageId.getAndIncrement()
    val numTasks = rdd.partitions.length
    val stage = new ShuffleMapStage(id, rdd, numTasks, parentStages,
      firstJobId, callSite, shuffleDep, batchRDDs, batchDeps)

    if (mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
      val serLocs = mapOutputTracker.getSerializedMapOutputStatuses(shuffleDep.shuffleId)
      val locs = MapOutputTracker.deserializeMapStatuses(serLocs)
      (0 until locs.length).foreach { i =>
        if (locs(i) ne null) {
          // locs(i) will be null if missing
          stage.addOutputLoc(i, locs(i))
        }
      }
    } else {
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      logInfo("Registering RDD " + rdd.id + " (" + rdd.getCreationSite + ") shuffle " +
        shuffleDep.shuffleId)
      mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.length)
      batchDeps.map { shuf =>
        logInfo("Registering RDD " + shuf.rdd.id + " (" + shuf.rdd.getCreationSite + ") shuffle " +
          shuf.shuffleId)
        mapOutputTracker.registerShuffle(shuf.shuffleId, shuf.rdd.partitions.length)
      }
    }

    shuffleIdToMapStage(shuffleDep.shuffleId) = stage
    batchDeps.foreach { shuf =>
      shuffleIdToMapStage(shuf.shuffleId) = stage
    }

    stageIdToStage(id) = stage
    // TODO: Remove this
    updateJobIdStageIdMaps(firstJobId, stage)
    stage
  }

  private def createDrizzleResultStage(
      rdd: RDD[_],
      batchRDDs: Seq[RDD[_]],
      func: (TaskContext, Iterator[_]) => _,
      batchFuncs: Seq[(TaskContext, Iterator[_]) => _],
      firstJobId: Int,
      partitions: Array[Int],
      callSite: CallSite,
      parentStages: List[Stage]): ResultStage = {
    val id = nextStageId.getAndIncrement()
    val stage = new ResultStage(id, rdd, func, partitions, parentStages, firstJobId,
      callSite, batchRDDs, batchFuncs)
    stageIdToStage(id) = stage
    // TODO: Remove this
    updateJobIdStageIdMaps(firstJobId, stage)
    stage
  }

  private def createBatchStages(
      finalRDD: RDD[_],
      batchRDDs: Seq[RDD[_]],
      stageMetadata: HashMap[Int, StageMetadata],
      batchStageMetadata: Seq[HashMap[Int, StageMetadata]],
      func: (TaskContext, Iterator[_]) => _,
      batchFuncs: Seq[(TaskContext, Iterator[_]) => _],
      partitions: Array[Int],
      jobId: Int,
      callSite: CallSite): ResultStage = {

    logDebug(s"Drizzle create batch stages. Got " +
      stageMetadata.mkString(",") + "\n" +
      batchStageMetadata.map(x => x.mkString(",")).mkString("\n"))

    // Create stages as described by the metadata.
    val waitingForStageCreation = new Stack[Seq[Int]]
    val waitingForVisit = new Stack[Seq[Int]]
    val idToStageMap = new HashMap[Int, Stage]

    waitingForVisit.push( Seq(finalRDD.id) ++ batchRDDs.map(x => x.id))

    while (!waitingForVisit.isEmpty) {
      val ids = waitingForVisit.pop()
      // Follow deps for each id here.
      waitingForStageCreation.push(ids)

      val deps = stageMetadata(ids(0)).deps
      val batchDeps = batchStageMetadata.zipWithIndex.map { x =>
        x._1(ids(x._2 + 1)).deps
      }

      for (i <- 0 until deps.size) {
        val batchToVisit = new ArrayBuffer[Int]()
        batchToVisit += deps(i)
        // Remove any RDDs that will be computed in this batch
        // from the deps so that we don't pick up redundant RDDs
        batchDeps.foreach { x =>
          x --= batchToVisit
          batchToVisit += x(i)
        }
        waitingForVisit.push(batchToVisit.toSeq)
      }
    }

    // Now walk down the stack and create stages
    while (!waitingForStageCreation.isEmpty) {
      val batchIds = waitingForStageCreation.pop()
      val smt = stageMetadata(batchIds(0))
      logDebug("Creating batch for ids " + batchIds.mkString(","))
      val stageCreated =
        if (smt.kind == StageType.SHUFFLE_MAP) {
          val parents = smt.deps.map(x => idToStageMap(x))
          val r = smt.rdd
          val batchR = batchStageMetadata.zip(batchIds.tail).map(x => x._1(x._2).rdd)

          val d = smt.shuffleDep.get
          val batchD = batchStageMetadata.zip(batchIds.tail).map(x => x._1(x._2).shuffleDep.get)
          createDrizzleShuffleMapStage(r, batchR, d, batchD, jobId, callSite, parents.toList)
        } else {
          val parents = smt.deps.map(x => idToStageMap(x))
          val r = smt.rdd
          val batchR = batchStageMetadata.zip(batchIds.tail).map(x => x._1(x._2).rdd)

          val f = smt.func.get
          val batchF = batchStageMetadata.zip(batchIds.tail).map(x => x._1(x._2).func.get)
          createDrizzleResultStage(r, batchR, f, batchF, jobId, partitions, callSite,
            parents.toList)
        }

      idToStageMap(batchIds(0)) = stageCreated
    }

    idToStageMap(finalRDD.id).asInstanceOf[ResultStage]
  }

  /** Mark a map stage job as finished with the given output stats, and report to its listener. */
  def markMapStageJobAsFinished(job: ActiveJob, stats: MapOutputStatistics): Unit = {
    // In map stage jobs, we only create a single "task", which is to finish all of the stage
    // (including reusing any previous map outputs, etc); so we just mark task 0 as done
    job.finished(0) = true
    job.numFinished += 1
    job.listener.taskSucceeded(0, stats)
    cleanupStateForJobAndIndependentStages(job)
    listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
  }

  def stop() {
    messageScheduler.shutdownNow()
    eventProcessLoop.stop()
    taskScheduler.stop()
  }

  eventProcessLoop.start()
}

private[scheduler] class DAGSchedulerEventProcessLoop(dagScheduler: DAGScheduler)
  extends EventLoop[DAGSchedulerEvent]("dag-scheduler-event-loop") with Logging {

  private[this] val timer = dagScheduler.metricsSource.messageProcessingTimer

  /**
   * The main event loop of the DAG scheduler.
   */
  override def onReceive(event: DAGSchedulerEvent): Unit = {
    val timerContext = timer.time()
    try {
      doOnReceive(event)
    } finally {
      timerContext.stop()
    }
  }

  private def doOnReceive(event: DAGSchedulerEvent): Unit = event match {
    case JobSubmitted(jobId, rdd, func, partitions, callSite, listener,
        properties, batchRDDs, batchFuncs) =>
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener,
          properties, batchRDDs, batchFuncs)

    case MapStageSubmitted(jobId, dependency, callSite, listener, properties) =>
      dagScheduler.handleMapStageSubmitted(jobId, dependency, callSite, listener, properties)

    case StageCancelled(stageId) =>
      dagScheduler.handleStageCancellation(stageId)

    case JobCancelled(jobId) =>
      dagScheduler.handleJobCancellation(jobId)

    case JobGroupCancelled(groupId) =>
      dagScheduler.handleJobGroupCancelled(groupId)

    case AllJobsCancelled =>
      dagScheduler.doCancelAllJobs()

    case ExecutorAdded(execId, host) =>
      dagScheduler.handleExecutorAdded(execId, host)

    case ExecutorLost(execId, reason) =>
      val filesLost = reason match {
        case SlaveLost(_, true) => true
        case _ => false
      }
      dagScheduler.handleExecutorLost(execId, filesLost)

    case BeginEvent(task, taskInfo) =>
      dagScheduler.handleBeginEvent(task, taskInfo)

    case GettingResultEvent(taskInfo) =>
      dagScheduler.handleGetTaskResult(taskInfo)

    case completion: CompletionEvent =>
      dagScheduler.handleTaskCompletion(completion)

    case TaskSetFailed(taskSet, reason, exception) =>
      dagScheduler.handleTaskSetFailed(taskSet, reason, exception)

    case ResubmitFailedStages =>
      dagScheduler.resubmitFailedStages()
  }

  override def onError(e: Throwable): Unit = {
    logError("DAGSchedulerEventProcessLoop failed; shutting down SparkContext", e)
    try {
      dagScheduler.doCancelAllJobs()
    } catch {
      case t: Throwable => logError("DAGScheduler failed to cancel all jobs.", t)
    }
    dagScheduler.sc.stop()
  }

  override def onStop(): Unit = {
    // Cancel any active jobs in postStop hook
    dagScheduler.cleanUpAfterSchedulerStop()
  }
}

private[spark] object DAGScheduler {
  // The time, in millis, to wait for fetch failure events to stop coming in after one is detected;
  // this is a simplistic way to avoid resubmitting tasks in the non-fetchable map stage one by one
  // as more failure events come in
  val RESUBMIT_TIMEOUT = 200
}
