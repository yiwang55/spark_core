package com.bilibili.seniororder

import org.apache.spark.{SparkConf, SparkContext}

object Demo34_TakeOrdered {
  def main(args: Array[String]): Unit = {
    //1. 入口
    val context = new SparkContext(new SparkConf()
      .setAppName("Demo33_Sort")
      .setMaster("local[*]")
    )

    //2. 准备数据并指定分区数
    //2. 数据
    //2.1 stu:id name gender age class
    val stuList = List(
      Student(1,"杨过",22, 180.00),
      Student(2,"郭靖",36, 170.00),
      Student(3,"黄蓉",38, 165.00),
      Student(4,"郭芙",21, 167.00),
      Student(5,"张无忌",23, 180.00),
      Student(6,"周芷若",21, 180.00)
    )

    // 需求：按照身高降序排序，身高相同，按照年龄升序排序。 --> 二次排序，取前五条件数据
    val stuRDD = context.parallelize(stuList)
    stuRDD.takeOrdered(5)(new Ordering[Student](){
      override def compare(previos: Student, next: Student): Int = {
        var ret = next.height.compareTo(previos.height)
        if (ret == 0) ret = previos.age.compareTo(next.age)
        ret
      }
    }).foreach(println)
  }
}
