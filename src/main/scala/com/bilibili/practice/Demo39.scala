package com.bilibili.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo39 {
  def main(args: Array[String]): Unit = {
    //1. 获取入口
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Demo38"))
    //2. 获取用户访问基站的日志
    val filesRDD:RDD[String] = sc.textFile("D:\\bigdata\\第二题数据-lacduration\\19735E1C66.log")
    //3. 切分日志:((phone,lac), time)
    val userInfoRDD:RDD[((String, String), Long)] = filesRDD.map(line => {
      val fields:Array[String] = line.split(",")
      val phone = fields(0)
      val time = fields(1).toLong
      val lac = fields(2)
      val eventType = fields(3)
      val time_long = if (eventType.equals("1")) -time else time
      ((phone, lac), time_long)
    })
    //4. 聚合
    val sumedRDD:RDD[((String, String), Long)] = userInfoRDD.reduceByKey(_ + _)

    //5. 为了便于和基站的基本信息进行join，我们要再次处理一下数据:(lac, (phone,time))
    val lacAndPT:RDD[(String, (String, Long))] = sumedRDD.map(tup => {
      val phone = tup._1._1
      val lac = tup._1._2
      val time = tup._2
      (lac,(phone, time))
    })

    //6. 加载基站信息
    val lacInfo = sc.textFile("D:\\bigdata\\第二题数据-lacduration\\lac_info.txt")

    //7. 切分基站信息
    val lacAndXY:RDD[(String, (String, String))] = lacInfo.map(line => {
      val fields = line.split(",")
      val lac = fields(0)
      val x = fields(1)
      val y = fields(2)
      (lac, (x, y))
    })

    //8. join
    val joinedRDD:RDD[(String, ((String,Long),(String, String)))] = lacAndPT.join(lacAndXY)

    //9. 为了方便以后的分组排序，需要进行数据整合
    val phoneAndTXY:RDD[(String, Long, (String, String))] = joinedRDD.map(tup => {
      val phone = tup._2._1._1
      val time = tup._2._1._2
      val xy = tup._2._2
      (phone, time, xy)
    })

    val value: RDD[(String, Iterable[(String, Long, (String, String))])] = phoneAndTXY.groupBy(_._1)
    //10. 按照手机进行分组
    val groupedRDD = value

    //11. 排序
    val sorted  = groupedRDD.mapValues(_.toList.sortBy(_._2).reverse)

    //12. 整合
    val res = sorted.map(tup => {
      val phone = tup._1
      val list = tup._2
      val filterlist = list.map(tup1 => {
        val time = tup1._2 // 时间长
        val xy = tup1._3
        (time, xy)
      })
      (phone, filterlist)
    })

    //13.
    val ress = res.mapValues(_.take(2))
    println(ress.collect().toList)

    sc.stop()
  }
}
