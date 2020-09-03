package com.yiwang.action

import org.apache.spark.{SparkConf, SparkContext}

//Action算子会触发所有Transformation算子
object ActionDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ActionDemo").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(2,1,3,6,5))
    //collect 将rdd转换成数组 或者集合的形式，并展示所有数据
    println(rdd1.collect().toList)

    //count 返回RDD中存储元素的个数
    println(rdd1.count())

    //top 取出RDD中的元素 top()
    //自带排序 默认是降序
    println(rdd1.top(2).toBuffer)

    //take
    //顺序取出rdd中的数据（没有排序）
    println(rdd1.take(3).toBuffer)
  }
}
