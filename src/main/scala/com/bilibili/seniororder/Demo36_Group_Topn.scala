package com.bilibili.seniororder

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Demo36_Group_Topn {
  def main(args: Array[String]): Unit = {
    //1. 入口
    val context = new SparkContext(new SparkConf()
      .setAppName("Demo36_Group_Topn")
      .setMaster("local[*]")
    )
    //2. 加载数据
    val linesRDD = context.textFile("d:\\bigdata\\wc.txt")

    //3. 改造数据
    val course2InfoRDD = linesRDD.map(line => {
      val spaceIndex = line.indexOf(",")
      val course = line.substring(0, spaceIndex)
      val info = line.substring(spaceIndex + 1)
      (course, info)
    })

    //4.分组
    val groupByKeyRDD = course2InfoRDD.combineByKey(createCombiner, mergeValue, mergeCombiner)

    //5. 分组内排序
    val sortedRDD = groupByKeyRDD.map{
      case (course, infos) => {
        val topN = mutable.TreeSet[String]()(new Ordering[String]() {
          override def compare(prevous_info: String, next_info: String): Int = {
            val prevousScore = prevous_info.split(",")(1).toInt // 获取前一个的分数
            val nextScore = next_info.split(",")(1).toInt // 获取后一个的分数
            var ret = nextScore.compareTo(prevousScore)
            if (ret ==0) {
              val prevousName = prevous_info.split(",")(0)
              val nextName = next_info.split(",")(0)
              ret = prevousName.compareTo(nextName)
            }
            ret
          }
        })

        for (info <- infos) {
          topN.add(info)
        }

        (course, topN.take(3))
      }
    }

    //6. 打印
    sortedRDD.foreach(println)
  }

  def createCombiner(info:String):ArrayBuffer[String] = {
    val ab = new ArrayBuffer[String]()
    ab.append(info)
    ab
  }

  def mergeValue(ab:ArrayBuffer[String], info:String):ArrayBuffer[String] = {
    ab.append(info)
    ab
  }

  def mergeCombiner(ab1:ArrayBuffer[String], ab2:ArrayBuffer[String]):ArrayBuffer[String] = {
    ab1.++:(ab2)
  }
}
