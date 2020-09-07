package com.yiwang.other

import org.apache.spark.{SparkConf, SparkContext}

object OtherDemo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OtherDemo2").setMaster("local")
    val sc = new SparkContext(conf)

    //遍历RDD中的每一个分区中的数据
    //多用于 数据进行持久化，例如,存储到数据库中
   val rdd1 = sc.parallelize(List(1,2,3,4,5,6,7,8,9),3)
    rdd1.foreachPartition(x => println(x.reduce(_+_)))

    //用传入的值做key，rdd原有的值做value，生成一个新的元组
    //keyBy可以遍历rdd每个元素
    val rdd2 = sc.parallelize(List("dog","cat","pig","bee","Sparrow"))
    val rdd2_1 = rdd2.keyBy(_.length)
    println(rdd2_1.collect().toList)

    //keys获取rdd中元组的所有key
    //values获取rdd中元组的所有value
    println(rdd2_1.keys.collect().toList)
    println(rdd2_1.values.collect().toList)

    //rdd中的元组，转换成Map
    val map: collection.Map[Int, String] = rdd2_1.collectAsMap()
    println(map)
  }
}