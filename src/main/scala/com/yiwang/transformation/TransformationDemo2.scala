package com.yiwang.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TransformationDemo2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().
      setAppName("TransformationDemo2").
      setMaster("local")
    val sc = new SparkContext(conf)

    //并集 union
    val rdd1 = sc.parallelize(List(5,6,7,8))
    val rdd2 = sc.parallelize(List(1,2,5,6))
    val rdd3 = rdd1.union(rdd2)
    println(rdd3.collect().toBuffer)

    //交集 intersection
    val rdd4 = rdd1 intersection rdd2
    println(rdd4.collect().toBuffer)

    //distinct 去重
    println(rdd3.distinct().collect().toBuffer)

    //join 对相等的key进行合并操作,生成key对元组RDD
    val rdd5: RDD[(String, Int)] = sc.parallelize(List(("tom", 1),("jerry", 3),("kitty",2)))
    val rdd6: RDD[(String, Int)] = sc.parallelize(List(("jerry", 2),("tom", 2),("dog",10)))

    val rdd7: RDD[(String, (Int, Int))] = rdd5 join rdd6
    println(rdd7.collect().toBuffer)
  }
}
