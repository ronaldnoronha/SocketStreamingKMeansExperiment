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
import org.apache.spark.util.{SizeEstimator, CollectionAccumulator}

import scala.util.Random
import java.time.LocalTime

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
    println("Application started at: "+LocalTime.now)

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("StreamingKmeans")
    val ssc = new StreamingContext(sparkConf, Seconds(args(4).toInt))

    val centers:Array[Vector] = new Array[Vector](8)
    for (i <- 0 to centers.length-1) {
      centers(i) = Vectors.dense(Array(Random.nextDouble, Random.nextDouble, Random.nextDouble).map(_*30-15))
    }

    val weights:Array[Double] = new Array[Double](centers.length)
    for (i<-0 to weights.length-1) {
      weights(i) = 1/centers.length
    }

    val model = new StreamingKMeansModel(centers,weights)

    def collectionAccumulator(name:String):CollectionAccumulator[Long] = {
      val acc = new CollectionAccumulator[Long]
      ssc.sparkContext.register(acc,name)
      acc
    }

    def collectionAccumulatorDouble(name:String):CollectionAccumulator[Double] = {
      val acc = new CollectionAccumulator[Double]
      ssc.sparkContext.register(acc,name)
      acc
    }

    val portStart = args(1).toInt
    val numPorts = args(2).toInt
    val host = args(0)

    val messages = (1 to numPorts).map { i =>
      ssc.socketTextStream(host, portStart+i-1, StorageLevel.MEMORY_AND_DISK_SER_2)
    }

    val lines = ssc.union(messages).flatMap(_.split(";"))

    val time = collectionAccumulator("Time")
    val rddCounter = collectionAccumulator("RDD Counter")
    val timeStamp = collectionAccumulator("Timestamp")


    val count = ssc.sparkContext.longAccumulator("Counter")
    val size = ssc.sparkContext.longAccumulator("Size Estimator")

    val computeCost = collectionAccumulatorDouble("Compute Cost")
    val trainingCost = collectionAccumulatorDouble("Training Cost")

    lines.foreachRDD(rdd => {
      val points = rdd.map(_.split(" ").map(_.toDouble)).map(x=>Vectors.dense(x))
      count.add(points.count)
      size.add(SizeEstimator.estimate(points))
      rddCounter.add(rdd.count())

      val time1 = System.currentTimeMillis()
      model.update(points, 1.0, "batches")

      time.add(System.currentTimeMillis()-time1)
      timeStamp.add(System.currentTimeMillis())
      computeCost.add(model.computeCost(points))
      trainingCost.add(model.trainingCost)
    })

    ssc.start()
    ssc.awaitTerminationOrTimeout(args(3).toInt)
    println("Application stopped at: "+LocalTime.now)

    println("Number of messages: "+ count.value)
    println("Size of the data: "+ size.value)

    var totalUpdateTime = 0L
    for (i <- 0 to time.value.size()-1) {
      totalUpdateTime += time.value.get(i)
    }
    println("Total update time: "+ totalUpdateTime)
    var totalMsgsProcessed = 0L
    for (i <- 0 to timeStamp.value.size()-1) {
      totalMsgsProcessed += rddCounter.value.get(i)
      println("Time elapsed " + timeStamp.value.get(i) + " messages processed " + totalMsgsProcessed)
    }
//    var totalMessages = 0.0
//    for (i <- model.clusterWeights) {
//      totalMessages += i
//    }
//    println("Total Messages: "+ totalMessages)
  }
}

// scalastyle:on println
