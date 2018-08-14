package com.atwy.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WordCountJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("WC_Java");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("C:\\Users\\yan\\Desktop\\大数据相关\\sparkfile\\spark.txt").cache();
        System.out.println("count  >>>>>>>>>>>>>>   :"+rdd1.count());
        /*FlatMapFunction  类型；；；现将 数组变为 list ，在将list 变成 iterator  */
        JavaRDD<String> rdd2 = rdd1.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        /*映射为 元组*/
        JavaPairRDD<String, Integer> rdd3 = rdd2.mapToPair(f -> new Tuple2(f, 1));
        /*reduce by key*/
        JavaPairRDD<String, Integer> rdd4 = rdd3.reduceByKey((v1, v2) -> v1 + v2);
        /*收集*/
        List<Tuple2<String, Integer>> list = rdd4.collect();
        /*打印*/
        for (Tuple2<String, Integer> t : list) {
            System.out.println(t._1 + "," + t._2);
        }
        sc.stop();
    }
}
