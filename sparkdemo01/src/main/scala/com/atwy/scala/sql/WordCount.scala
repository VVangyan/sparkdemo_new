package com.atwy.scala.sql

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local").setAppName("WC")
    val sc = new SparkContext(config)

    val rdd1 = sc.textFile("C:\\Users\\yan\\Desktop\\大数据相关\\sparkfile\\spark.txt").cache()

    val rdd2 = rdd1.flatMap(line => line.split(" "))
    val rdd3 = rdd2.map((_, 1))

    val rdd4 = rdd3.reduceByKey(_ + _)

    rdd4.foreach(println)
    sc.stop()
  }
}
