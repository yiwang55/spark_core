package com.yiwang.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//Spark版本的wordcount
object SparkWordCountRun {
  def main(args: Array[String]): Unit = {
    //需要对Spark进行配置
    val conf = new SparkConf().setAppName("SparkWordCount")
    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //读取文件
    val lines: RDD[String] = sc.textFile(args(0))
    //切分数据
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //将单词组成元组
    val tuples:RDD[(String,Int)] = words.map((_,1))
    //spark中所提供的一个算子,这个算子可以将相同key的value进行求和
    val sumed: RDD[(String, Int)] = tuples.reduceByKey(_+_)
    //对当前的结果进行排序  false代表的是降序排序
    val sorted: RDD[(String, Int)] = sumed.sortBy(_._2,false)
    //将当前数据提交到集群中存储
    sorted.saveAsTextFile(args(1))

    sc.stop()


  }
}
