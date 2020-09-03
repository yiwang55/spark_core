package com.yiwang.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object TransformationDemo5 {
  def main(args: Array[String]): Unit = {
       val conf = new SparkConf().setAppName("TransformationDemo5").setMaster("local")
       val sc = new SparkContext(conf)

       val rdd1 = sc.parallelize(List(1,2,3,4,5,6),3)
       println("初始化分区值:"+rdd1.partitions.length)

      //repartition可以对rdd进行重新分区,即可以是超过原有分区个数,也可以小于原有分区个数
      //repartition默认会执行 shuffle  即对分区中的数据重新计算
      val rdd1_1 = rdd1.repartition(5)
      println("rdd通过repartition算子调整分区后的分区数:"+rdd1_1.partitions.length)

      //coalesce 算子也可以对rdd进行重新分区,这个分区只能是小于原有分区的值
      //coalesce是不会进行shuffle 默认是不会进行shuffle的 所以不能修改比原有分区数大
      val rdd1_2 = rdd1.coalesce(2)
      println("rdd通过coalesce算子调整分区后的分区数:"+rdd1_2.partitions.length)

      //即RDD中存储的元素是元组类型,此时可以使用partitionby进行重新分区
      val rdd2 = sc.parallelize(List(("e",5),("c",3),("d",4),("c",2)),2)
     //参数是 默认分区器即 HashPartitioner  也可以传入自定义分区器
      val rdd2_1: RDD[(String, Int)] = rdd2.partitionBy(new HashPartitioner(4))
      println("rdd2通过partitionBy算子调整分区后的分区数:"+rdd2_1.partitions.length)

     //repartitionAndSortWithinPartitions是repartition算子的一个变种
    // 官方建议在使用完repartition算子重新分区之后,还需要进行排序
     //既可以重新分区又可以进行排序
    //这个算子只能对元组进行排序
      val rdd3 = sc.parallelize(List((3,"cc"),(4,"bb"),(1,"aa")),2)
      rdd3.repartitionAndSortWithinPartitions(new HashPartitioner(1)).foreach(println)



  }
}
