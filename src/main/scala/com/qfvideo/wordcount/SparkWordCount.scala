package com.qfvideo.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkWordCount")
    val context: SparkContext = new SparkContext(conf)
    val lines: RDD[String] = context.textFile("data.txt")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val tuples: RDD[(String, Int)] = words.map((_,1))

    val unit: RDD[(String, Int)] = tuples.reduceByKey(_+_)
    unit.foreach(println)
  }
}
