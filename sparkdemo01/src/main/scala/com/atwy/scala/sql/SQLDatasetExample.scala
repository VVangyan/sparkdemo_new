package com.atwy.scala.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SQLDatasetExample {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setAppName("SQLDatasetExample").setMaster("local")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config(sc)
      .getOrCreate()

    //getMysqlData(spark)
    //getMysqlData02(spark)
    getMysqlData03(spark)
    spark.stop()
  }

  /*通过read  format 方式*/
  def getMysqlData01(spark: SparkSession): Unit = {
    val jdbcDF = spark.sqlContext.read
      .format("jdbc")
      .option("url", "jdbc:mysql:///world")
      .option("dbtable", "city")
      .option("user", "admin")
      .option("password", "admin")
      .load()
    jdbcDF.show()
  }

  /*spark.sqlContext.read   与spark.read  均可以读取数据*/
  def getMysqlData02(spark: SparkSession): Unit = {
    val connectionProperties = new Properties()
    connectionProperties.put("user", "admin")
    connectionProperties.put("password", "admin")
    val jdbcDF = spark.read.jdbc("jdbc:mysql:///world", "city", connectionProperties)
    //jdbcDF.show()
    jdbcDF.show(100)
  }
  def getMysqlData03(spark: SparkSession): Unit = {
    val connectionProperties = new Properties()
    connectionProperties.put("user", "admin")
    connectionProperties.put("password", "admin")
    val jdbcDF = spark.read.jdbc("jdbc:mysql:///world", "city", connectionProperties)
    //jdbcDF.show()
    jdbcDF.show(100)
  }


}
