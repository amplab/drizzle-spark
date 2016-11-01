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

// scalastyle:off println
package org.apache.spark.examples

import scala.math.random

import org.apache.spark._

object DrizzleRunningSum {

  def main(args: Array[String]) {

    val numElemsPerPart = 1000L

    val numIters = if (args.length > 0) args(0).toInt else 10
    val batchSize = if (args.length > 1) args(1).toInt else 5 // if batchSize is 1 it runs baseline
    val partitions = if (args.length > 2) args(2).toInt else 4
    val numTrials = if (args.length > 3) args(3).toInt else 5
    val numElems = if (args.length > 4) args(4).toLong else (numElemsPerPart * partitions)
    val numReducers = if (args.length > 5) args(5).toInt else 2

    val conf = new SparkConf().setAppName("DrizzleRunningSum")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // Let all the executors join
    Thread.sleep(10000)
    // Warm up the JVM and copy the JAR out to all the machines etc.
    val execIds = sc.parallelize(0 until sc.getExecutorMemoryStatus.size,
      sc.getExecutorMemoryStatus.size).foreach { x =>
      Thread.sleep(1)
    }

    (0 until numTrials).foreach { trial =>
      val startIter = System.nanoTime()
      val partitioner = new HashPartitioner(numReducers)
      val rdd = sc.parallelize(1L to numElems, partitions).cache()

      var stateRDD = sc.parallelize(0 until numReducers, numReducers).map(x => (x, 0L)).cache()

      val batchRDDs = (0 until numIters).map { i =>
        val dataRDD = rdd.map { x =>
          val inc = x + i*numElems
          (inc % numReducers, inc)
        }
        stateRDD = dataRDD.groupByKey(partitioner).zip(stateRDD).map { y =>
          (y._2._1, y._2._2 + y._1._2.sum)
        }
        stateRDD.cache()
        stateRDD
      }

      val beginMillis = System.currentTimeMillis
      val pairCollectFunc = (iter: Iterator[(Int, Long)]) => {
        iter.map(i => (i._1, i._2)).toArray
      }

      val begin = System.nanoTime

      if (batchSize == 1) {
        (0 until numIters).foreach { i =>
          val sums = sc.runJob(batchRDDs(i), pairCollectFunc)
          println(sums.map(x => x.mkString(",")).mkString("\n"))
        }
      } else {
        val numBatches = math.ceil(numIters.toDouble / batchSize).toInt
        batchRDDs.grouped(batchSize).foreach { batch =>
          val funcs = Seq.fill(batch.size)(pairCollectFunc)
          val outs = sc.runJobs(batch, funcs)
          outs.zipWithIndex.foreach { case (out, idx) =>
            val strOut = out.zipWithIndex.map { x =>
              x._1.zipWithIndex.map { y =>
                y._1.toString
              }.mkString(",")
            }.mkString("\n")
            println(strOut)
          }
        }
      }
      val end = System.nanoTime
      println("Drizzle: Running " + numIters + " iters " + batchSize + " batchSize took " +
        (end-begin)/1e6 + " ms. Creating took " + (begin-startIter)/1e6 + " ms")
    }

    sc.stop()
  }
}
