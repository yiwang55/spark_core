package com.qfvideo.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TransformationDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().
      setAppName("TransformationDemo").
      setMaster("local")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10))

    val rdd2: RDD[Int] = rdd.map(_ * 2)
    //直接打印rdd不会有任何结果，因为只使用了转换操作
    println(rdd2)
    //使用了行动操作collect后，就可以打印出具体的结果
    println(rdd2.collect().toBuffer)

    //filter rdd内元素大于10的值，返回新的rdd
    val rdd3: RDD[Int] = rdd2.filter(_ > 10)
    print(rdd3.collect().toBuffer)

    //flatMap
    val rdd4: RDD[String] = sc.parallelize(Array("a b c","b c d"))
    val rdd5: RDD[String] = rdd4.flatMap(_.split(" "))
    print(rdd5.collect().toBuffer)

    //sample
    val rdd6 = sc.parallelize(1 to 10)
    //第一个参数：表示抽出的数据是否放回 true:放回 false:不放回
    //第二个参数：抽样比例  浮点型 30% : 0.3
    //第三个参数：种子，默认值 不传入的
    val rdd7 = rdd6.sample(false, 0.5)
    print(rdd7.collect().toBuffer)

  }
}
