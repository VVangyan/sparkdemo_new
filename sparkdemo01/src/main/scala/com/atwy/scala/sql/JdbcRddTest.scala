package com.atwy.scala.sql

import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.JdbcRDD

object JdbcRddTest {


  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[2]").setAppName("JdbcRddTest")
    val sc = new SparkContext(config)
    val rdd = new JdbcRDD(
      sc,
      () => {
        //Class.forName("com.mysql.jdbc.Driver").newInstance()
        DriverManager.getConnection("jdbc:mysql:///world", "admin", "admin")
      },
      "select * from  city where id>= ? and id<= ?",
      1,
      100,
      1,
      r => (r.getInt(1), r.getString(2))
    )
    println(rdd.count())
    rdd.foreach(println(_))
    sc.stop()
  }
}
