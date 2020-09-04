package com.yiwang.action

import org.apache.spark.{SparkConf, SparkContext}

object ActionDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ActionDemo1").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(3,1,4,8,5,4,6))
    //top 降序排序，取出数据
    //takeOrdered 默认asc排序，取出数据
    println(rdd1.takeOrdered(3).toBuffer)

    //first = take(1)
    println(rdd1.first())

    //rdd存储成文件
    //路径可一是本地路径或者hdfs路径
//    rdd1.saveAsTextFile("/dir")

    //和map的区别
    //foreach没有返回值
    rdd1.foreach(println)
  }
}
