package com.bilibili.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 数据格式：
 *
 * 19735E1C66.log ： 存储的日志信息
 * 手机号码，时间戳，基站id，连接状态(1连接，0断开)
 * 18688888888,20160327082400,16030401EAFB68F1E3CDF819735E1C66,1
 *
 * lac_info.txt ： 存储基站信息
 * 基站id，经度，纬度
 * 9F36407EAD8829FC166F14DDE7970F68,116.304864,40.050645,6
 *
 * 需求：
 * 根据用户产生的日志信息，分析在哪个基站停留的时间最长
 * 在一定范围内，求所有用户经过的所有基站所停留时间最长的TOP2
 */
object Demo39 {
  def main(args: Array[String]): Unit = {
    //1. 获取入口
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Demo39"))
    //2. 获取用户访问基站的日志
      val filesRDD: RDD[String] = sc.textFile("D:\\bigdata\\codefiles\\第二题数据-lacduration\\19735E1C66.log")
    //3. 切分日志:((phone,lac), time)
    val userInfoRDD: RDD[((String, String), Long)] = filesRDD.map(line => {
      val fields = line.split("\\,")
      val phone = fields(0)
      val lac = fields(2)
      val eventType = fields(3)
      val time = fields(1).toLong
      //time需要计算一下 为下一步reduceByKey做准备
      if ("1".equals(eventType)) -time else time

      ((phone, lac), time)
    })
    //4. 聚合
    val sumedRDD: RDD[((String, String), Long)] = userInfoRDD.reduceByKey(_ + _)

    //5. 为了便于和基站的基本信息进行join，我们要再次处理一下数据:(lac, (phone,time))
    val lacAndPT: RDD[(String, (String, Long))] = sumedRDD.map(e => {
      val lac = e._1._2
      val phone = e._1._1
      val time = e._2
      (lac, (phone, time))
    })

    //6. 加载基站信息
    val lacInfo: RDD[String] = sc.textFile("D:\\bigdata\\codefiles\\第二题数据-lacduration\\lac_info.txt")
    //7. 切分基站信息
    val lacAndXY:RDD[(String, (String, String))] = lacInfo.map(line => {
      val fields = line.split(",")
      val lac = fields(0)
      val x = fields(1)
      val y = fields(2)
      (lac, (x, y))
    })

    //8. join
      val joinedRDD: RDD[(String, ((String, Long), (String, String)))] =
        lacAndPT.join(lacAndXY)

    //9. 为了方便以后的分组排序，需要进行数据整合(phone, time, xy)
    val phoneAndTXY: RDD[(String, Long, (String, String))] = joinedRDD.map(tup => {
      val phone = tup._2._1._1
      val time = tup._2._1._2
      val xy = tup._2._2

      (phone, time, xy)
    })
    //10. 按照手机进行分组
    val groupedRDD: RDD[(String, Iterable[(String, Long, (String, String))])]
    = phoneAndTXY.groupBy(_._1)

    //11. 排序
    val sorted: RDD[(String, List[(String, Long, (String, String))])]
    = groupedRDD.mapValues(_.toList.sortBy(_._2).reverse)

    //12. 整合(phone, filterlist)
    val res: RDD[(String, List[(Long, (String, String))])] = sorted.map(tup => {
      val phone = tup._1
      val list = tup._2
      val filterlist = list.map(e => {
        val time = e._2
        val xy = e._3
        (time, xy)
      })
      (phone, filterlist)
    })

    //13.
    val ress = res.mapValues(_.take(2))
    println(ress.collect().toList)

    //14.释放资源
    sc.stop()
  }
}
