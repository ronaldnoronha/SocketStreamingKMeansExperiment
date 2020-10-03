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

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.clustering.StreamingKMeansModel
import org.apache.spark.util.SizeEstimator

import scala.util.Random

/**
 * Custom Receiver that receives data over a socket. Received bytes are interpreted as
 * text and \n delimited lines are considered as records. They are then counted and printed.
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example org.apache.spark.examples.streaming.CustomReceiver localhost 9999`
 */
object Experiment {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: CustomReceiver <hostname> <port>")
      System.exit(1)
    }

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("CustomReceiver")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val centers:Array[Vector] = new Array[Vector](8)
    for (i <- 0 to centers.length-1) {
      centers(i) = Vectors.dense(Array(Random.nextDouble, Random.nextDouble, Random.nextDouble).map(_*30-15))
    }

    val weights:Array[Double] = new Array[Double](centers.length)
    for (i<-0 to weights.length-1) {
      weights(i) = 1/centers.length
    }

    val model = new StreamingKMeansModel(centers,weights)

    val portStart = args(1).split(",")(0).toInt
    val portEnd = args(1).split(",")(1).toInt
    val host = args(0)

    val messages = (portStart to portEnd).map { i =>
      ssc.socketTextStream(host, i, StorageLevel.MEMORY_AND_DISK_SER_2)
    }

    val lines = ssc.union(messages).flatMap(_.split(";"))

    val count = ssc.sparkContext.longAccumulator("Counter")
    val size = ssc.sparkContext.longAccumulator("Size Estimator")

    lines.foreachRDD(rdd => {
      val points = rdd.map(_.split(" ").map(_.toDouble)).map(x=>Vectors.dense(x))
      count.add(points.count)
      size.add(SizeEstimator.estimate(points))
      model.update(points, 1.0, "batches")
    })


    ssc.start()
    ssc.awaitTerminationOrTimeout(60000)
    println("Number of messages: "+ count.value)
    println("Size of the data: "+ size.value)

    for (i <- model.clusterWeights) {
      println(i)
    }
  }
}

// scalastyle:on println
