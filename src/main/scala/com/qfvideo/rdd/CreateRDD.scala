package com.qfvideo.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CreateRDD {
  def main(args: Array[String]): Unit = {
    /**
     * Spark的四种运行模式
     * 本地模式, Standalone模式, Spark on Yarn模式, Spark on Mesos模式
     */
    ////进行配置， setMaster("local")本地运行模式
    val conf = new SparkConf().setAppName("CreateRDD").setMaster("local")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    //常用创建RDD的三种方法
    val rdd1: RDD[Int] = sc.makeRDD(Array(1,2,3))
    val rdd2: RDD[Int] = sc.parallelize(Array(1,2,3))
    val rdd3: RDD[String] = sc.textFile("local path或者HDFS path")

  }
}
