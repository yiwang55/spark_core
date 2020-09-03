package com.yiwang.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TransformationDemo3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().
      setAppName("TransformationDemo3").
      setMaster("local")
    val sc = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.parallelize(List(("tom", 1),("jerry", 3),("kitty",2)))
    val rdd2: RDD[(String, Int)] = sc.parallelize(List(("jerry", 2),("tom", 2),("dog",10)))

    //左连接
    val rdd3: RDD[(String, (Int, Option[Int]))] = rdd1 leftOuterJoin rdd2
    println(rdd3.collect().toBuffer)

    //右连接
    val rdd4: RDD[(String, (Option[Int], Int))] = rdd1 rightOuterJoin rdd2
    println(rdd4.collect().toBuffer)

    //笛卡尔积
    val rdd5: RDD[((String, Int), (String, Int))] = rdd1 cartesian rdd2
    println(rdd5.collect().toBuffer)

    //groupBy 分组  根据传入的参数进行分组 ,相同参数是一组
    val rdd6 = sc.parallelize(List(("tom",1),("jerry",3),("kitty",2),("jerry",2),("tom",2),("dog",10)))
    val rdd7: RDD[(String, Iterable[(String, Int)])] = rdd6.groupBy(_._1)
    println(rdd7.collect.toBuffer)
  }
}
