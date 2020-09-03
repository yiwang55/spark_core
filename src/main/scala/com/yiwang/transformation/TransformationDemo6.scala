package com.yiwang.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TransformationDemo6 {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("TransformationDemo6").setMaster("local")
      val sc = new SparkContext(conf)

      val rdd1 = sc.parallelize(List(1,2,3,4,5,6,7))
      //reduce可以对rdd中的元素进行求和
       val sum = rdd1.reduce(_+_)
      println("rdd1中元素的和:"+sum)

     //reduceBykey 是存储着元素类型的rdd ,相同key为一组,计算对应的value值
     val rdd2 = sc.parallelize(List(("cat",2),("cat",5),("pig",10),("dog",3),("dog",4)))
      val sum2:RDD[(String,Int)] = rdd2.reduceByKey(_+_)
      println("rdd2中元素的和:"+sum2.collect().toList)

     //aggregateBykey 主要应对的是需要默认值并且需要计算分区值的计算方式

    val sum3: RDD[(String, Int)] = rdd2.aggregateByKey(1)(_+_,_+_)
     println("rdd2中元素的和:"+sum3.collect.toList)

    //combineByKey
    val sum4: RDD[(String, Int)] = rdd2.combineByKey(x => x, (a:Int, b:Int)=>a+b, (m:Int, n:Int)=> m+n)
    println("rdd2中元素的和:"+sum4.collect.toList)

  }
}
