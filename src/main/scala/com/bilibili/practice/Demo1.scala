package com.bilibili.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo1 {

  def main(args: Array[String]): Unit = {
    //1. 获取入口
    val sparkContext = new SparkContext(new SparkConf().setAppName("Demo1").setMaster("local[*]"))
    //2. 加载数据
    val logs = sparkContext.textFile("d:\\bigdata\\Advert.txt")
    //3. 切
    val arrayRdd: RDD[Array[String]] = logs.map(e => e.split("\\s+"));
    //4. 处理数据：(provice_adid, 1)
    val proAndAdId: RDD[(String, Int)] = arrayRdd.map(e => (e(1) + "_" + e(4), 1))
    //5. 将每个身份每个广告的所有点击量进行聚合
    val proAndAdCntRDD: RDD[(String, Int)] = proAndAdId.reduceByKey(_ + _)
    //6. 扩大粒度，分拆key，RDD[pro, (adid, count)]
    val proAndAdCnt2RDD: RDD[(String, (String, Int))] = proAndAdCntRDD.map(e => {
      val strings = e._1.split("_")
      (strings(0), (strings(1), e._2))
    })
    //7. 分组:RDD[(pro, (adid,cnt))]
    val pro2AdArrayRDD: RDD[(String, Iterable[(String, Int)])] = proAndAdCnt2RDD.groupByKey()
    //8. 获取前三:(adid,cnt)【String，Int】
    val result = pro2AdArrayRDD.mapValues(values => values.toList.sortWith((current, next) => current._2 > next._2).take(3))
    //9. 变成map
      val map = result.collectAsMap()
    println(map)
    //10. 释放资源
    sparkContext.stop()
  }
}


