package com.atwy.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object SparkStreamDemo {
  def main(args: Array[String]): Unit = {
    // Create a local StreamingContext with four working thread and batch interval of 1 second
    val config = new SparkConf().setMaster("local[4]").setAppName("SparkStreamDemo")
    val ssc = new StreamingContext(config, Seconds(6))
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("192.168.1.168", 9999)
    // Split each line into words
    val words = lines.flatMap(_.split(" "))
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate


  }
}
