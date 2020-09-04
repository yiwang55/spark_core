package com.yiwang.other

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object OtherDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OtherDemo1").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(("e",5),("c",2),("d",4),("c",2),("a",1)))
    //countByKey
    //生成一个Map, map key = key, value  = count
    val keys = rdd1.countByKey()
    println(keys)

    //将rdd中元素 看做整体，统计相同的value
    val values: collection.Map[(String, Int), Long] = rdd1.countByValue()
    println(values)

    //过滤并返回指定范围（包括开始和结束的，相当于between）
    val ranges: RDD[(String, Int)] = rdd1.filterByRange("a","c")
    println(ranges.collect().toBuffer)
  }
}
